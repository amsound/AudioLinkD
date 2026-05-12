use anyhow::{anyhow, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::{
    traits::{Consumer, Observer, Producer, Split},
    HeapRb,
};
use rubato::{FastFixedIn, PolynomialDegree, Resampler};use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::convert::TryInto;
use std::net::UdpSocket;
use std::process::Command;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::sync::{
    atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};

use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};

// ─── Constants ────────────────────────────────────────────────────────────────

const SAMPLE_RATE: u32 = 48_000;
const FRAME_SAMPLES: usize = 960;       // 20ms at 48kHz
const FRAME_MS: u32 = 20;
const MAX_CHANNELS: usize = 64;
const PORT: u16 = 20102;

const SSRC_CONTROL: u32 = 0x0000_0200;
const SSRC_METADATA: u32 = 0x0000_0000;
const SSRC_MEDIA: u32 = 0x0001_0000;

// Development defaults: both peers match if neither side passes --token.
// For real deployments pass the same --token on both ends and a different --id per node.
const DEFAULT_SHARED_TOKEN: [u8; 16] = [0xa5; 16];


const PRIME_SAMPLES: usize = 960 * 6;  // 120ms
const MIN_LATENCY_MS: u32 = 5;
const MAX_LATENCY_MS: u32 = 10_000;
const MIN_EFFECTIVE_RX_BUFFER_MS: u32 = FRAME_MS * 2;
const PHASE_LOCK_TIMEOUT_MS: u64 = 10;
const STATS_LOG_INTERVAL: Duration = Duration::from_secs(5);
const CAP_RING_SIZE: usize = (SAMPLE_RATE as usize * 200) / 1000;
const PB_RING_SIZE: usize = SAMPLE_RATE as usize;
const REPRIME_AFTER_EMPTY_CALLBACKS: u32 = 30;
const UNDERRUN_DECAY: f32 = 0.9;
const ALSA_PERIOD: cpal::BufferSize = cpal::BufferSize::Default;

// EBU R49 line-up tone: 1 kHz at -18 dBFS peak.
const TONE_AMPLITUDE: f32 = 0.125_892_54; // -18 dBFS peak

const TX_SRC_EBU_L: usize = 100;
const TX_SRC_EBU_R: usize = 101;
const TX_SRC_INPUT_BASE: usize = 1000;
const TX_SRC_SILENT: usize = usize::MAX;

// ─── Source ───────────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum Source {
    Matrix, // matrix-routed EBU/local-input sources
}

// ─── Packet format ────────────────────────────────────────────────────────────

fn build_packet(seq: u16, timestamp: u32, channel: u8, opus: &[u8]) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(19 + opus.len());
    pkt.push(0x80);
    pkt.push(0x69);
    pkt.extend_from_slice(&seq.to_be_bytes());
    pkt.extend_from_slice(&timestamp.to_be_bytes());
    pkt.extend_from_slice(&SSRC_MEDIA.to_be_bytes());
    pkt.push(0x09);
    pkt.push(0x06);
    pkt.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);
    pkt.push(channel);
    pkt.extend_from_slice(opus);
    pkt
}

#[derive(Clone, Copy)]
struct MediaPacket<'a> {
    seq: u16,
    timestamp: u32,
    channel: u8,
    opus_payload: &'a [u8],
}

/// Returns parsed RTP-style AudioLink media packet fields.
fn parse_packet(data: &[u8]) -> Option<MediaPacket<'_>> {
    if data.len() < 19 {
        return None;
    }
    if data[0] != 0x80 || data[1] != 0x69 {
        return None;
    }
    if be_u32_at(data, 8)? != SSRC_MEDIA {
        return None;
    }
    if data[12] != 0x09 || data[13] != 0x06 {
        return None;
    }
    Some(MediaPacket {
        seq: u16::from_be_bytes(data.get(2..4)?.try_into().ok()?),
        timestamp: be_u32_at(data, 4)?,
        channel: data[18],
        opus_payload: &data[19..],
    })
}


fn rtp_header(ssrc: u32) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(64);
    pkt.push(0x80);
    pkt.push(0x69);
    pkt.extend_from_slice(&0u16.to_be_bytes());
    pkt.extend_from_slice(&0u32.to_be_bytes());
    pkt.extend_from_slice(&ssrc.to_be_bytes());
    pkt
}

fn build_probe_packet(device_name_token: &[u8; 16], shared_token: &[u8; 16]) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x07]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(device_name_token);
    pkt.extend_from_slice(shared_token);
    pkt
}

fn build_accept_packet(shared_token: &[u8; 16]) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x09]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(shared_token);
    pkt
}

fn build_confirm_packet(shared_token: &[u8; 16]) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x08]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(shared_token);
    pkt.extend_from_slice(shared_token);
    pkt
}

/// 09 0b — RTT Ping. Carries a u64 microsecond send timestamp.
/// Receiver echoes it back unchanged as a 09 0c Pong.
fn build_rtt_ping(timestamp_us: u64) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x0b]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(&timestamp_us.to_be_bytes());
    pkt
}

/// 09 0c — RTT Pong. Echoes the timestamp from the ping unchanged.
fn build_rtt_pong(timestamp_us: u64) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x0c]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(&timestamp_us.to_be_bytes());
    pkt
}

fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

fn default_node_id() -> String {
    if let Ok(v) = std::env::var("HOSTNAME") {
        let v = v.trim();
        if !v.is_empty() {
            return v.to_string();
        }
    }
    if let Ok(out) = Command::new("hostname").output() {
        if out.status.success() {
            let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !v.is_empty() {
                return v;
            }
        }
    }
    "unknown-host".to_string()
}

fn build_metadata_packet_with_labels(
    device_name_token: &[u8; 16],
    num_channels: usize,
    node_id: &str,
    labels: &[String],
) -> Vec<u8> {
    let mut json = format!(r#"{{"id":"{}","channels":["#, json_escape(node_id));
    for ch in 0..num_channels {
        if ch > 0 {
            json.push(',');
        }
        let label = labels.get(ch).cloned().unwrap_or_else(|| local_channel_label(ch));
        json.push_str(&format!(r#"{{"c":{},"l":"{}"}}"#, ch + 1, json_escape(&label)));
    }
    json.push_str("]}");

    let json_bytes = json.as_bytes();
    let body_len = 16usize + json_bytes.len();
    let len24 = (body_len as u32).min(0x00ff_ffff);

    let mut pkt = rtp_header(SSRC_METADATA);
    pkt.extend_from_slice(&[0x09, 0x0a]);
    pkt.extend_from_slice(&[0x00, 0x01, 0x01, 0x00]);
    pkt.push(((len24 >> 16) & 0xff) as u8);
    pkt.push(((len24 >> 8) & 0xff) as u8);
    pkt.push((len24 & 0xff) as u8);
    pkt.extend_from_slice(device_name_token);
    pkt.extend_from_slice(json_bytes);
    pkt
}

fn build_metadata_packet(device_name_token: &[u8; 16], num_channels: usize, node_id: &str) -> Vec<u8> {
    let labels: Vec<String> = (0..num_channels).map(local_channel_label).collect();
    build_metadata_packet_with_labels(device_name_token, num_channels, node_id, &labels)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
struct RemoteMetadata {
    node_id: Option<String>,
    channels: usize,
    labels: Vec<String>,
}

impl RemoteMetadata {
    fn display_name(&self) -> &str {
        self.node_id.as_deref().unwrap_or("unknown device")
    }
}

#[derive(Debug)]
enum HandshakePacket {
    Probe { sender_token: [u8; 16], expected_peer: [u8; 16] },
    Accept { token: [u8; 16] },
    Confirm { token: [u8; 16] },
    Metadata(RemoteMetadata),
    RttPing { timestamp_us: u64 },
    RttPong { timestamp_us: u64 },
}

fn be_u32_at(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(data.get(offset..offset + 4)?.try_into().ok()?))
}

fn copy_token(data: &[u8], offset: usize) -> Option<[u8; 16]> {
    Some(data.get(offset..offset + 16)?.try_into().ok()?)
}

fn parse_json_string_field(json: &str, field: &str) -> Option<String> {
    let needle = format!("\"{field}\":\"");
    let start = json.find(&needle)? + needle.len();
    let mut out = String::new();
    let mut escape = false;
    for c in json[start..].chars() {
        if escape {
            match c {
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                other => out.push(other),
            }
            escape = false;
        } else if c == '\\' {
            escape = true;
        } else if c == '"' {
            return Some(out);
        } else {
            out.push(c);
        }
    }
    None
}

fn parse_channel_metadata(json: &str) -> RemoteMetadata {
    let mut max_channel = 0usize;
    let mut labels = Vec::new();

    for entry in json.split('{').skip(1) {
        let entry = entry.split('}').next().unwrap_or(entry);
        if let Some(c_pos) = entry.find("\"c\":") {
            let digits: String = entry[c_pos + 4..]
                .chars()
                .skip_while(|c| c.is_whitespace())
                .take_while(|c| c.is_ascii_digit())
                .collect();
            if let Ok(c) = digits.parse::<usize>() {
                max_channel = max_channel.max(c);
            }
        }

        if let Some(l_pos) = entry.find("\"l\":\"") {
            let rest = &entry[l_pos + 5..];
            if let Some(end) = rest.find('"') {
                labels.push(rest[..end].to_string());
            }
        }
    }

    RemoteMetadata {
        node_id: parse_json_string_field(json, "id"),
        channels: max_channel.min(MAX_CHANNELS),
        labels,
    }
}

fn parse_handshake_packet(data: &[u8]) -> Option<HandshakePacket> {
    if data.len() < 14 || data[0] != 0x80 || data[1] != 0x69 {
        return None;
    }

    let ssrc = be_u32_at(data, 8)?;
    let kind = data.get(12..14)?;

    match (ssrc, kind) {
        (SSRC_CONTROL, [0x09, 0x07]) if data.len() >= 53 => {
            // Probe body: 7 reserved bytes, sender token (offset 21), shared/expected token (offset 37).
            Some(HandshakePacket::Probe {
                sender_token: copy_token(data, 21)?,
                expected_peer: copy_token(data, 37)?,
            })
        }
        (SSRC_CONTROL, [0x09, 0x09]) if data.len() >= 37 => {
            Some(HandshakePacket::Accept { token: copy_token(data, 21)? })
        }
        (SSRC_CONTROL, [0x09, 0x08]) if data.len() >= 53 => {
            Some(HandshakePacket::Confirm { token: copy_token(data, 21)? })
        }
        (SSRC_METADATA, [0x09, 0x0a]) if data.len() >= 37 => {
            // Metadata body: flags[4], len[3], source token[16], JSON bytes.
            let body_len = ((data[18] as usize) << 16) | ((data[19] as usize) << 8) | data[20] as usize;
            let json_start = 37;
            let json_end = if body_len >= 16 {
                (json_start + body_len - 16).min(data.len())
            } else {
                data.len()
            };
            let json = std::str::from_utf8(data.get(json_start..json_end)?).ok()?;
            let metadata = parse_channel_metadata(json);
            Some(HandshakePacket::Metadata(metadata))
        }
        // 09 0b — RTT Ping: 7 reserved bytes then u64 timestamp.
        (SSRC_CONTROL, [0x09, 0x0b]) if data.len() >= 29 => {
            let ts = u64::from_be_bytes(data.get(21..29)?.try_into().ok()?);
            Some(HandshakePacket::RttPing { timestamp_us: ts })
        }
        // 09 0c — RTT Pong: echoed timestamp.
        (SSRC_CONTROL, [0x09, 0x0c]) if data.len() >= 29 => {
            let ts = u64::from_be_bytes(data.get(21..29)?.try_into().ok()?);
            Some(HandshakePacket::RttPong { timestamp_us: ts })
        }
        _ => None,
    }
}

/// Derive the shared link token from both device names and an optional password.
/// Both sides sort the names alphabetically before hashing, so the token is
/// identical regardless of which side is initiator and which is responder.
/// Neither the names nor the password are transmitted — both sides derive locally.
/// Reserved as key material for future AES-256-GCM transport encryption.
fn derive_link_token(my_device_name: &str, remote_device_name: &str, link_password: Option<&str>) -> [u8; 16] {
    let (a, b) = if my_device_name <= remote_device_name {
        (my_device_name, remote_device_name)
    } else {
        (remote_device_name, my_device_name)
    };
    match link_password.filter(|p| !p.is_empty()) {
        Some(pw) => derive_token_from_text(&format!("{a}:{b}:{pw}")),
        None      => derive_token_from_text(&format!("{a}:{b}")),
    }
}

fn derive_token_from_text(text: &str) -> [u8; 16] {
    let mut a: u64 = 0xcbf2_9ce4_8422_2325;
    let mut b: u64 = 0x8422_2325_cbf2_9ce4;
    for &byte in text.as_bytes() {
        a ^= byte as u64;
        a = a.wrapping_mul(0x0000_0100_0000_01b3);
        b ^= (byte as u64).rotate_left(1);
        b = b.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let mut out = [0u8; 16];
    out[..8].copy_from_slice(&a.to_be_bytes());
    out[8..].copy_from_slice(&b.to_be_bytes());
    out
}

fn parse_token_arg(value: &str) -> Result<[u8; 16]> {
    let cleaned: String = value.chars().filter(|c| *c != '-').collect();
    if cleaned.len() == 32 && cleaned.chars().all(|c| c.is_ascii_hexdigit()) {
        let mut out = [0u8; 16];
        for i in 0..16 {
            out[i] = u8::from_str_radix(&cleaned[i * 2..i * 2 + 2], 16)?;
        }
        Ok(out)
    } else {
        Ok(derive_token_from_text(value))
    }
}

fn token_to_hex(token: &[u8; 16]) -> String {
    token.iter().map(|b| format!("{b:02x}")).collect::<String>()
}

fn generated_link_token(node_id: &str) -> [u8; 16] {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let seed = format!("audiolinkd:{node_id}:{}:{now}", std::process::id());
    derive_token_from_text(&seed)
}

fn split_host_port(value: &str) -> String {
    value.strip_suffix(&format!(":{PORT}")).unwrap_or(value).to_string()
}

// ─── Encoder factory ──────────────────────────────────────────────────────────

fn make_encoder_with_bitrate(bitrate_per_channel: u32) -> Result<opus::Encoder> {
    let mut enc =
        opus::Encoder::new(SAMPLE_RATE, opus::Channels::Mono, opus::Application::Audio)?;
    enc.set_bitrate(opus::Bitrate::Bits(bitrate_per_channel as i32))?;
    enc.set_inband_fec(false)?;
    Ok(enc)
}

fn make_encoder() -> Result<opus::Encoder> {
    make_encoder_with_bitrate(128_000)
}

// ─── Stereo mixdown ───────────────────────────────────────────────────────────

/// Mix N decoded channel buffers down to stereo interleaved output.
///
/// Routing:
///   odd-indexed channels  (0, 2, 4, ...) → L
///   even-indexed channels (1, 3, 5, ...) → R
///
/// Channels are routed at unity gain into the stereo bus.
///
/// This means a single active source remains at its source level even when N > 2
/// (for example, a -18 dBFS peak tone remains -18 dBFS peak after fold-down).
/// If several channels on the same side are active at once they sum electrically
/// and can exceed the level of one channel; that is intentional for this baseline
/// and keeps the mixer from applying hidden attenuation.
///
/// Special cases:
///   N=1 → ch0 copied to both L and R at unity (mono sum)
///   N=2 → direct 1:1 map, no division (1 contributor each side)
///
/// `channels` is a slice of FRAME_SAMPLES-length PCM buffers, one per channel.
/// `out` is a stereo interleaved output buffer of length 2 × FRAME_SAMPLES.
fn mixdown(channels: &[Vec<f32>], out: &mut [f32]) {
    let n = channels.len();
    assert_eq!(out.len(), FRAME_SAMPLES * 2);

    if n == 0 {
        out.fill(0.0);
        return;
    }

    if n == 1 {
        // Mono: ch0 → L and R at unity.
        for (i, s) in channels[0].iter().enumerate() {
            out[i * 2]     = *s;
            out[i * 2 + 1] = *s;
        }
        return;
    }

    // N ≥ 2: split even channels to L, odd channels to R at unity gain.

    for i in 0..FRAME_SAMPLES {
        let mut l = 0.0f32;
        let mut r = 0.0f32;
        for (ch, buf) in channels.iter().enumerate() {
            if ch % 2 == 0 {
                l += buf[i];
            } else {
                r += buf[i];
            }
        }
        out[i * 2]     = l;
        out[i * 2 + 1] = r;
    }
}

// ─── Re-prime logic ───────────────────────────────────────────────────────────

fn handle_underrun(all_empty: bool, started: &AtomicBool, empty_counter: &AtomicU32) {
    if all_empty {
        let count = empty_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= REPRIME_AFTER_EMPTY_CALLBACKS {
            tracing::warn!("Output dry for ~{}ms — re-priming", count * 20);
            started.store(false, Ordering::Relaxed);
            empty_counter.store(0, Ordering::Relaxed);
        }
    } else {
        empty_counter.store(0, Ordering::Relaxed);
    }
}


// ─── M6 timestamped jitter buffer helpers ────────────────────────────────────

#[derive(Clone)]
pub struct JitterConfig {
    configured_delay_ms: u32,
    target_delay_ms: u32,
    adaptive: bool,
    phase_lock: bool,
}

struct FrameGroup {
    packets: Vec<Option<Vec<u8>>>,
    expected: HashSet<u8>,
    received_at: Instant,
}

impl FrameGroup {
    fn new(_timestamp: u32, expected_channels: usize) -> Self {
        let expected = (0..expected_channels.min(MAX_CHANNELS))
            .map(|ch| ch as u8)
            .collect();
        Self {
            packets: vec![None; MAX_CHANNELS],
            expected,
            received_at: Instant::now(),
        }
    }

    fn insert(&mut self, channel: usize, payload: &[u8]) {
        if channel < MAX_CHANNELS {
            self.packets[channel] = Some(payload.to_vec());
        }
    }

    fn complete(&self) -> bool {
        self.expected
            .iter()
            .all(|&ch| self.packets[ch as usize].is_some())
    }

    fn timed_out(&self) -> bool {
        self.received_at.elapsed() >= Duration::from_millis(PHASE_LOCK_TIMEOUT_MS)
    }
}

fn timestamp_elapsed_samples(newer: u32, older: u32) -> u32 {
    newer.wrapping_sub(older)
}

fn latency_ms_to_samples(ms: u32) -> u32 {
    ((ms as u64 * SAMPLE_RATE as u64) / 1000) as u32
}

fn effective_receive_buffer_ms(configured_ms: u32) -> u32 {
    configured_ms
        .clamp(MIN_LATENCY_MS, MAX_LATENCY_MS)
        .max(MIN_EFFECTIVE_RX_BUFFER_MS)
}

fn sleep_until(deadline: Instant) {
    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline - now;
        if remaining > Duration::from_millis(2) {
            std::thread::sleep(remaining - Duration::from_millis(1));
        } else {
            std::thread::yield_now();
        }
    }
}

fn ring_fill_ms<T: Observer>(rings: &[T], active_channels: usize) -> usize {
    if active_channels == 0 {
        return 0;
    }
    let min_fill = rings[..active_channels]
        .iter()
        .map(|r| r.occupied_len())
        .min()
        .unwrap_or(0);
    min_fill * 1000 / SAMPLE_RATE as usize
}

fn decode_or_plc(decoder: &mut opus::Decoder, payload: Option<&[u8]>, pcm: &mut [f32]) -> bool {
    match payload {
        Some(data) if decoder.decode_float(data, pcm, false).is_ok() => true,
        _ => {
            if decoder.decode_float(&[], pcm, true).is_err() {
                pcm.fill(0.0);
            }
            false
        }
    }
}

fn drain_phase_locked_groups(
    groups: &mut BTreeMap<u32, FrameGroup>,
    decoders: &mut [opus::Decoder],
    remote_channels: usize,
    decoded: &mut [Vec<f32>],
    pcm: &mut [f32],
    on_group: &mut dyn FnMut(&[Vec<f32>], usize),
) -> (usize, usize) {
    let active = remote_channels.min(MAX_CHANNELS);
    if active == 0 {
        return (0, 0);
    }

    // Drain strictly oldest-first. A complete oldest group is ready immediately.
    // An incomplete oldest group becomes ready after the 10ms phase-lock timeout
    // and missing/corrupt channels are generated with Opus PLC.
    let mut output_groups = 0usize;
    let mut plc_channels = 0usize;

    loop {
        let ready_ts = match groups.iter().next() {
            Some((&ts, group)) if group.complete() || group.timed_out() => ts,
            _ => break,
        };

        let Some(group) = groups.remove(&ready_ts) else { continue; };
        for ch in 0..active {
            let was_real = decode_or_plc(
                &mut decoders[ch],
                group.packets[ch].as_deref(),
                pcm,
            );
            if !was_real {
                plc_channels += 1;
            }
            decoded[ch].copy_from_slice(pcm);
        }
        on_group(decoded, active);
        output_groups += 1;
    }

    (output_groups, plc_channels)
}



fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ─── M7 web UI + patch matrix state ─────────────────────────────────────────

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum MonitorMode {
    /// Confirmed M6 compatibility monitor: mono copies ch0; N>=2 alternates even→L, odd→R at unity.
    CompatStereoAlternate,
    /// M7 patch-matrix monitor: routes network receive channels to physical output L/R.
    PatchMatrix,
}

impl MonitorMode {
    fn as_u8(self) -> u8 {
        match self {
            Self::CompatStereoAlternate => 0,
            Self::PatchMatrix => 1,
        }
    }

    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::PatchMatrix,
            _ => Self::CompatStereoAlternate,
        }
    }

    fn parse(value: &str) -> Result<Self> {
        match value {
            "compat" | "compat-stereo" | "alternate" | "stereo" => Ok(Self::CompatStereoAlternate),
            "matrix" | "patch" | "patch-matrix" => Ok(Self::PatchMatrix),
            other => Err(anyhow!("Unknown --monitor '{other}'. Use: compat or matrix")),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PeerStatus {
    Gray,
    Green,
    Orange,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Route {
    source: String,
    destination: String,
}


#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct PersistedRuntimeConfig {
    remote: Option<String>,
    remote_device_name: Option<String>,
    link_password: Option<String>,
    node_id: Option<String>,
    token_hex: Option<String>,
    channels: Option<usize>,
    opus_bitrate_per_channel: Option<u32>,
    latency_ms: Option<u32>,
    fixed_jitter: Option<bool>,
    phase_lock: Option<bool>,
    channel_labels: Option<Vec<String>>,
    rendezvous_url: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct PersistedUiState {
    #[serde(default)]
    config: PersistedRuntimeConfig,
    #[serde(default)]
    routes: Vec<Route>,
}

fn persisted_state_path() -> std::path::PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join("audiolinkd_config.json")
}

fn legacy_persisted_state_path() -> std::path::PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join(".audiolinkd_m7_state.json")
}

fn load_persisted_state() -> PersistedUiState {
    for path in [persisted_state_path(), legacy_persisted_state_path()] {
        let Ok(text) = std::fs::read_to_string(&path) else { continue; };
        match serde_json::from_str::<PersistedUiState>(&text) {
            Ok(state) => return state,
            Err(e) => tracing::warn!("Ignoring unreadable AudioLink config at {}: {e}", path.display()),
        }
    }
    PersistedUiState::default()
}

fn save_persisted_state(state: &PersistedUiState) {
    match serde_json::to_string_pretty(state) {
        Ok(text) => {
            if let Err(e) = std::fs::write(persisted_state_path(), text) {
                tracing::warn!("Could not save AudioLink config: {e}");
            }
        }
        Err(e) => tracing::warn!("Could not serialise AudioLink config: {e}"),
    }
}

fn save_persisted_routes(routes: &[Route]) {
    let mut state = load_persisted_state();
    state.routes = routes.to_vec();
    save_persisted_state(&state);
}

fn save_persisted_config(config: PersistedRuntimeConfig) {
    let mut state = load_persisted_state();
    // Preserve channel labels — they are saved separately via local_labels_post_handler
    // and must not be wiped when the engine rebuilds due to a setup change.
    let preserved_labels = state.config.channel_labels.clone();
    state.config = config;
    state.config.channel_labels = preserved_labels;
    save_persisted_state(&state);
}

fn route_valid_for_runtime(route: &Route, local_inputs: usize, send_channels: usize) -> bool {
    if let Some(dst) = parse_endpoint_channel(&route.destination, "stream:0:ch:") {
        if dst >= send_channels { return false; }
        if route.source == "ebu:l" || route.source == "ebu:r" { return true; }
        return parse_endpoint_channel(&route.source, "input:").map(|ch| ch < local_inputs).unwrap_or(false);
    }
    if let Some(dst) = parse_endpoint_channel(&route.destination, "output:") {
        if dst >= 2 { return false; }
        return parse_endpoint_channel(&route.source, "peer:remote:ch:").is_some();
    }
    false
}

fn load_persisted_routes(local_inputs: usize, send_channels: usize) -> Vec<Route> {
    load_persisted_state()
        .routes
        .into_iter()
        .filter(|r| route_valid_for_runtime(r, local_inputs, send_channels))
        .collect()
}

/// Load channel labels from persisted config, merging with defaults for any
/// channels beyond what was saved. This preserves names across engine restarts
/// and when the channel count increases. Reducing channels simply stops at the
/// new count — names of removed channels are discarded as expected.
fn load_persisted_labels(num_channels: usize) -> Vec<String> {
    let saved = load_persisted_state().config.channel_labels.unwrap_or_default();
    (0..num_channels)
        .map(|ch| {
            saved.get(ch)
                .filter(|l| !l.trim().is_empty())
                .cloned()
                .unwrap_or_else(|| local_channel_label(ch))
        })
        .collect()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Endpoint {
    id: String,
    label: String,
    kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
struct UiStats {
    channels: usize,
    fill_ms: usize,
    target_ms: u32,
    phase_lock: bool,
    queued_groups: usize,
    decoded_fps: f64,
    output_underflows: usize,
    plc_channels: usize,
    seq_missing: usize,
    loss_percent: f64,
    jitter_ms: f64,
    latency_ms: usize,
    rx_mbps: f64,
    tx_mbps: f64,
    drift_pressure_ppm: isize,
    tx_fps: f64,
    tx_active_channel: usize,
    tx_peak_dbfs: Vec<f32>,
    input_peak_dbfs: Vec<f32>,
    rx_peak_dbfs: Vec<f32>,
    monitor_peak_dbfs: [f32; 2],
    rtt_ms: f64,
    one_way_latency_ms: f64,
    ring_overflows: usize,
}

#[derive(Clone, Debug, Serialize)]
struct RuntimeSummary {
    mode: String,
    remote_host: String,
    remote_device_name: String,
    source: String,
    codec: String,
    opus_bitrate_per_channel: u32,
    frame_ms: u32,
    tx_channels: usize,
    token_configured: bool,
    token_hint: String,
    #[serde(skip_serializing)]
    token_hex: String,
    send_enabled: bool,
    recv_enabled: bool,
    latency_ms: u32,
    effective_latency_ms: u32,
    fixed_jitter: bool,
    phase_lock: bool,
    link_password_configured: bool,
    rendezvous_url: String,
    web_note: String,
}


#[derive(Clone, Debug, Serialize)]
struct DeviceResponse {
    sample_rate: u32,
    default_input: String,
    default_output: String,
    default_input_channels: usize,
    default_output_channels: usize,
    inputs: Vec<String>,
    outputs: Vec<String>,
}

fn scan_audio_devices_once() -> DeviceResponse {
    let host = cpal::default_host();
    let default_input_device = host.default_input_device();
    let default_output_device = host.default_output_device();
    let default_input_channels = default_input_device
        .as_ref()
        .and_then(|d| d.default_input_config().ok())
        .map(|c| c.channels() as usize)
        .unwrap_or(0);
    let default_output_channels = default_output_device
        .as_ref()
        .and_then(|d| d.default_output_config().ok())
        .map(|c| c.channels() as usize)
        .unwrap_or(0);
    let default_input = default_input_device
        .and_then(|d| d.name().ok())
        .unwrap_or_else(|| "none".into());
    let default_output = default_output_device
        .and_then(|d| d.name().ok())
        .unwrap_or_else(|| "none".into());
    let inputs = host
        .input_devices()
        .map(|ds| ds.filter_map(|d| d.name().ok()).collect())
        .unwrap_or_default();
    let outputs = host
        .output_devices()
        .map(|ds| ds.filter_map(|d| d.name().ok()).collect())
        .unwrap_or_default();
    DeviceResponse {
        sample_rate: SAMPLE_RATE,
        default_input,
        default_output,
        default_input_channels,
        default_output_channels,
        inputs,
        outputs,
    }
}

struct MeterBank {
    tx_peak_db_x100: Vec<AtomicI32>,
    rx_peak_db_x100: Vec<AtomicI32>,
    input_peak_db_x100: Vec<AtomicI32>,
    monitor_peak_db_x100: [AtomicI32; 2],
}

impl MeterBank {
    fn new() -> Self {
        Self {
            tx_peak_db_x100: (0..MAX_CHANNELS).map(|_| AtomicI32::new(-12000)).collect(),
            rx_peak_db_x100: (0..MAX_CHANNELS).map(|_| AtomicI32::new(-12000)).collect(),
            input_peak_db_x100: (0..MAX_CHANNELS).map(|_| AtomicI32::new(-12000)).collect(),
            monitor_peak_db_x100: [AtomicI32::new(-12000), AtomicI32::new(-12000)],
        }
    }

    fn set_tx_peak(&self, ch: usize, db: f32) {
        if ch < self.tx_peak_db_x100.len() { self.tx_peak_db_x100[ch].store((db * 100.0).round() as i32, Ordering::Relaxed); }
    }

    fn set_rx_peak(&self, ch: usize, db: f32) {
        if ch < self.rx_peak_db_x100.len() { self.rx_peak_db_x100[ch].store((db * 100.0).round() as i32, Ordering::Relaxed); }
    }

    fn set_input_peak(&self, ch: usize, db: f32) {
        if ch < self.input_peak_db_x100.len() { self.input_peak_db_x100[ch].store((db * 100.0).round() as i32, Ordering::Relaxed); }
    }

    fn set_monitor_peak(&self, side: usize, db: f32) {
        if side < 2 { self.monitor_peak_db_x100[side].store((db * 100.0).round() as i32, Ordering::Relaxed); }
    }

    fn snapshot_tx(&self, n: usize) -> Vec<f32> { self.tx_peak_db_x100.iter().take(n.min(MAX_CHANNELS)).map(|v| v.load(Ordering::Relaxed) as f32 / 100.0).collect() }
    fn snapshot_rx(&self, n: usize) -> Vec<f32> { self.rx_peak_db_x100.iter().take(n.min(MAX_CHANNELS)).map(|v| v.load(Ordering::Relaxed) as f32 / 100.0).collect() }
    fn snapshot_input(&self, n: usize) -> Vec<f32> { self.input_peak_db_x100.iter().take(n.min(MAX_CHANNELS)).map(|v| v.load(Ordering::Relaxed) as f32 / 100.0).collect() }
    fn snapshot_monitor(&self) -> [f32; 2] { [self.monitor_peak_db_x100[0].load(Ordering::Relaxed) as f32 / 100.0, self.monitor_peak_db_x100[1].load(Ordering::Relaxed) as f32 / 100.0] }
}

fn peak_dbfs_from_peak(peak: f32) -> f32 {
    if peak <= 0.000_001 { -120.0 } else { (20.0 * peak.abs().log10()).clamp(-120.0, 0.0) }
}

#[derive(Clone)]
struct WebState {
    started_at: Instant,
    node_id: String,
    local_channels: usize,
    local_input_channels: usize,
    send_enabled: bool,
    recv_enabled: bool,
    handshake_connected: Arc<AtomicBool>,
    last_control_ms: Arc<AtomicU64>,
    last_audio_ms: Arc<AtomicU64>,
    remote_channels: Arc<AtomicUsize>,
    remote_metadata: Arc<Mutex<Option<RemoteMetadata>>>,
    monitor_mode: Arc<AtomicU8>,
    // Two physical stereo outputs, each represented by a 64-bit mask of network receive channels.
    output_route_masks: Arc<[AtomicU64; 2]>,
    // One transmit source mask per network send channel.
    tx_tone_source_for_send: Arc<Vec<AtomicUsize>>,
    local_labels: Arc<Mutex<Vec<String>>>,
    metadata_socket: Arc<UdpSocket>,
    device_name_token: [u8; 16],
    routes: Arc<Mutex<Vec<Route>>>,
    presets: Arc<Mutex<HashMap<String, Vec<Route>>>>,
    stats: Arc<Mutex<UiStats>>,
    meters: Arc<MeterBank>,
    runtime: RuntimeSummary,
    devices: Arc<DeviceResponse>,
    restart_lock: Arc<Mutex<()>>,
    /// RTT in microseconds × 10 (gives 0.1ms resolution without f64 atomic).
    rtt_us10: Arc<AtomicU32>,
    /// Name of a conflicting remote device that attempted to connect while already connected.
    remote_conflict: Arc<Mutex<Option<String>>>,
}


impl WebState {
    fn peer_status(&self) -> PeerStatus {
        let now_ms = now_millis();
        let last_control = self.last_control_ms.load(Ordering::Relaxed);
        let last_audio = self.last_audio_ms.load(Ordering::Relaxed);
        let control_age_ms = now_ms.saturating_sub(last_control);
        let audio_age_ms = now_ms.saturating_sub(last_audio);

        if !self.handshake_connected.load(Ordering::Relaxed) || last_control == 0 {
            return PeerStatus::Gray;
        }

        // Keepalive is sent roughly every 2130 ms. After about 5 seconds with no
        // control packet, treat the remote connected device as unavailable.
        if control_age_ms > 5_000 {
            return PeerStatus::Gray;
        }

        // If the link is established but media has not arrived or has gone stale, show degraded.
        if self.recv_enabled && ((last_audio == 0 && control_age_ms > 1_000) || (last_audio > 0 && audio_age_ms > 1_000)) {
            return PeerStatus::Orange;
        }

        if let Ok(stats) = self.stats.lock() {
            // Only mark degraded for *recent* transport/playout faults. These UI
            // counters are updated as per-stats-window deltas below. Historical
            // PLC from priming, channel-count changes, or an earlier route churn
            // must not leave the connected device permanently orange.
            if stats.output_underflows > 0 || stats.seq_missing > 0 {
                return PeerStatus::Orange;
            }
        }

        PeerStatus::Green
    }

    fn monitor_mode(&self) -> MonitorMode {
        MonitorMode::from_u8(self.monitor_mode.load(Ordering::Relaxed))
    }
}

#[derive(Debug, Deserialize)]
struct RoutesRequest {
    routes: Vec<Route>,
    monitor_mode: Option<MonitorMode>,
}

#[derive(Debug, Deserialize)]
struct PresetRequest {
    name: String,
}

#[derive(Debug, Deserialize)]
struct LocalLabelsRequest {
    labels: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct SetupApplyRequest {
    remote: String,
    remote_device_name: String,
    link_password: Option<String>,
    node_id: String,
    token: Option<String>,
    channels: usize,
    opus_bitrate_per_channel: u32,
    receive_buffer_ms: u32,
    rendezvous_url: Option<String>,
}

#[derive(Debug, Serialize)]
struct SetupApplyResponse {
    status: String,
    command: Vec<String>,
}

#[derive(Debug, Serialize)]
struct StatusResponse {
    node_id: String,
    uptime_seconds: u64,
    peer_status: PeerStatus,
    monitor_mode: MonitorMode,
    local_channels: usize,
    local_input_channels: usize,
    remote_channels: usize,
    send_enabled: bool,
    recv_enabled: bool,
    remote: Option<RemoteMetadata>,
    runtime: RuntimeSummary,
    last_control_age_ms: u64,
    last_audio_age_ms: u64,
    remote_conflict: Option<String>,
}

#[derive(Debug, Serialize)]
struct MatrixResponse {
    monitor_mode: MonitorMode,
    sources: Vec<Endpoint>,
    destinations: Vec<Endpoint>,
    routes: Vec<Route>,
}

fn parse_endpoint_channel(id: &str, prefix: &str) -> Option<usize> {
    id.strip_prefix(prefix)?.parse::<usize>().ok().filter(|&ch| ch < MAX_CHANNELS)
}

fn apply_routes_to_masks(routes: &[Route], masks: &[AtomicU64; 2]) {
    let mut out_masks = [0u64; 2];
    for route in routes {
        let Some(src) = parse_endpoint_channel(&route.source, "peer:remote:ch:") else { continue; };
        let Some(dst) = parse_endpoint_channel(&route.destination, "output:") else { continue; };
        if dst < 2 {
            out_masks[dst] |= 1u64 << src;
        }
    }
    masks[0].store(out_masks[0], Ordering::Relaxed);
    masks[1].store(out_masks[1], Ordering::Relaxed);
}

fn tx_source_code_from_endpoint(id: &str) -> Option<usize> {
    match id {
        "ebu:l" => Some(TX_SRC_EBU_L),
        "ebu:r" => Some(TX_SRC_EBU_R),
        _ => parse_endpoint_channel(id, "input:").map(|ch| TX_SRC_INPUT_BASE + ch),
    }
}

fn tx_source_bit(source_code: usize) -> Option<u64> {
    match source_code {
        TX_SRC_EBU_L => Some(1u64 << 0),
        TX_SRC_EBU_R => Some(1u64 << 1),
        code if code >= TX_SRC_INPUT_BASE && code < TX_SRC_INPUT_BASE + 62 => {
            Some(1u64 << (2 + (code - TX_SRC_INPUT_BASE)))
        }
        _ => None,
    }
}

fn source_code_from_bit_index(bit: usize) -> Option<usize> {
    match bit {
        0 => Some(TX_SRC_EBU_L),
        1 => Some(TX_SRC_EBU_R),
        b if b >= 2 && b < 64 => Some(TX_SRC_INPUT_BASE + (b - 2)),
        _ => None,
    }
}

fn apply_routes_to_tx_sources(routes: &[Route], tx_sources: &[AtomicUsize]) {
    // No implicit transmit routing. A network send channel is silent unless the
    // operator explicitly patches one or more sources to it on the Transmit Routing page.
    let mut masks = vec![0u64; tx_sources.len()];
    for route in routes {
        let Some(src_code) = tx_source_code_from_endpoint(&route.source) else { continue; };
        let Some(bit) = tx_source_bit(src_code) else { continue; };
        let Some(dst) = parse_endpoint_channel(&route.destination, "stream:0:ch:") else { continue; };
        if dst < masks.len() {
            masks[dst] |= bit;
        }
    }
    for (slot, mask) in tx_sources.iter().zip(masks.into_iter()) {
        slot.store(mask as usize, Ordering::Relaxed);
    }
}

fn sine_at(sample_index: u64, freq_hz: f32, amplitude: f32) -> f32 {
    // Keep phase bounded before calling sin(). Casting a huge absolute phase to
    // f32 was enough to make long-running test tones audibly degrade. This
    // preserves the M6 phase-wrapping fix for all generated tones.
    let cycles = (sample_index as f64 * freq_hz as f64 / SAMPLE_RATE as f64).fract();
    (amplitude as f64 * (cycles * std::f64::consts::TAU).sin()) as f32
}

fn periodic_pos_s(sample_index: u64, period_s: f64) -> f64 {
    let period_samples = (SAMPLE_RATE as f64 * period_s).round() as u64;
    (sample_index % period_samples) as f64 / SAMPLE_RATE as f64
}

fn tx_source_sample(source_code: usize, sample_index: u64, _active_rotating_tone: usize) -> f32 {
    match source_code {
        TX_SRC_EBU_L => {
            // EBU R49: channel 1/left is a 1 kHz line-up tone at -18 dBFS,
            // interrupted for 250 ms every 3 seconds.
            let pos = periodic_pos_s(sample_index, 3.0);
            if pos < 0.25 { 0.0 } else { sine_at(sample_index, 1000.0, TONE_AMPLITUDE) }
        }
        TX_SRC_EBU_R => sine_at(sample_index, 1000.0, TONE_AMPLITUDE),
        _ => 0.0,
    }
}

fn tx_source_peak_estimate(source_code: usize, frame_start_sample: u64, _active_rotating_tone: usize) -> f32 {
    let mut peak = 0.0f32;
    for offset in (0..FRAME_SAMPLES).step_by(12) {
        peak = peak.max(tx_source_sample(source_code, frame_start_sample + offset as u64, 0).abs());
    }
    peak
}

fn default_routes_for_channels(_channels: usize) -> Vec<Route> {
    // Start with a completely unpatched router. The EBU generator runs at boot
    // and shows signal on the Transmit Routing source rows, but nothing is sent
    // or monitored until the operator makes crosspoints.
    Vec::new()
}

fn local_channel_label(ch: usize) -> String {
    // Local channel naming is intentionally centralised here so the Web UI,
    // transmitted metadata and future editable labels can use the same source.
    // M7B will replace these defaults with runtime-editable names and trigger
    // a fresh 09 0a metadata packet when they change.
    format!("Ch {}", ch + 1)
}

fn matrix_for_state(state: &WebState) -> MatrixResponse {
    let local_labels = state.local_labels.lock().map(|l| l.clone()).unwrap_or_default();
    let remote = state.remote_metadata.lock().ok().and_then(|m| m.clone());
    let remembered_remote_channels = state.remote_channels.load(Ordering::Relaxed).min(MAX_CHANNELS);
    let remote_channels = remote
        .as_ref()
        .map(|m| m.channels.min(MAX_CHANNELS))
        .unwrap_or(remembered_remote_channels);
    let device_name = remote
        .as_ref()
        .and_then(|m| m.node_id.clone())
        .unwrap_or_else(|| "Connected Device".to_string());

    let mut sources = Vec::new();

    // Transmit-side sources. Keep this deliberately simple for now: the
    // stable routable line-up generator is EBU R49 L/R.
    sources.push(Endpoint { id: "ebu:l".into(), label: "EBU L".into(), kind: "test_tone".into() });
    sources.push(Endpoint { id: "ebu:r".into(), label: "EBU R".into(), kind: "test_tone".into() });
    for ch in 0..state.local_input_channels {
        sources.push(Endpoint {
            id: format!("input:{ch}"),
            label: format!("Local Input {}", ch + 1),
            kind: "physical_input".into(),
        });
    }

    // Receive-side sources, populated from 09 0a metadata when present.
    for ch in 0..remote_channels {
        let label = remote
            .as_ref()
            .and_then(|m| m.labels.get(ch).cloned())
            .unwrap_or_else(|| format!("Ch {}", ch + 1));
        sources.push(Endpoint {
            id: format!("peer:remote:ch:{ch}"),
            label: format!("{device_name} {label}"),
            kind: "network_receive".into(),
        });
    }


    // Fixed to stereo for now because these VMs expose two output channels.
    // This will become device-driven once audio device configuration is live.
    let mut destinations = vec![
        Endpoint { id: "output:0".into(), label: "Local Output 1".into(), kind: "physical_output".into() },
        Endpoint { id: "output:1".into(), label: "Local Output 2".into(), kind: "physical_output".into() },
    ];
    for ch in 0..state.local_channels {
        let label = local_labels.get(ch).cloned().unwrap_or_else(|| local_channel_label(ch));
        destinations.push(Endpoint {
            id: format!("stream:0:ch:{ch}"),
            label: format!("Send {} — {}", ch + 1, label),
            kind: "network_send".into(),
        });
    }
    let routes = state.routes.lock().map(|r| r.clone()).unwrap_or_default();
    MatrixResponse { monitor_mode: state.monitor_mode(), sources, destinations, routes }
}

async fn index_handler(State(_state): State<WebState>) -> Html<String> {
    Html(r##"<!doctype html>
<html lang="en-GB">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>AudioLink Control</title>
<style>
:root{--bg:#101418;--panel:#171d23;--ink:#e8eef4;--muted:#8fa0ad;--line:#2b3741;--green:#20c363;--orange:#f2a23a;--red:#e84b4b;--blue:#5ca8ff;--cell:#202a33}*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.45 system-ui,-apple-system,Segoe UI,sans-serif}header{display:flex;align-items:center;justify-content:space-between;padding:14px 20px;border-bottom:1px solid var(--line);background:#0d1115;position:sticky;top:0;z-index:6}h1{font-size:18px;margin:0;letter-spacing:.05em;text-transform:uppercase}.node{color:var(--muted);font-size:13px}nav{display:flex;gap:8px;padding:12px 20px;background:#12181e;border-bottom:1px solid var(--line);position:sticky;top:61px;z-index:5}button,select,input{background:#0f151b;color:var(--ink);border:1px solid var(--line);border-radius:8px;padding:8px 10px}button{cursor:pointer}button.tab.active{background:var(--blue);color:#07111b;border-color:var(--blue)}main{padding:20px;max-width:1500px;margin:auto}.page{display:none}.page.active{display:block}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:14px}.card{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:16px;box-shadow:0 10px 30px rgba(0,0,0,.16)}.card h2,.card h3{margin:0 0 12px}.kv{display:grid;grid-template-columns:1fr auto;gap:7px 12px}.kv span:nth-child(odd){color:var(--muted)}.status{display:inline-flex;align-items:center;gap:8px}.lamp{width:12px;height:12px;border-radius:50%;background:#66717b;box-shadow:0 0 0 2px rgba(255,255,255,.04)}.lamp.green{background:var(--green);box-shadow:0 0 12px var(--green)}.lamp.orange{background:var(--orange);box-shadow:0 0 12px var(--orange)}.lamp.red{background:var(--red);box-shadow:0 0 12px var(--red)}.remotealert{display:none;margin:0 0 16px;padding:13px 15px;border:1px solid var(--red);border-radius:12px;background:rgba(232,75,75,.18);color:#ffd0d0;font-weight:800}.remotealert.show{display:block}.remotealert.orange{border-color:var(--orange);background:rgba(242,162,58,.15);color:#ffe4bd}.topalert{display:none;position:sticky;top:111px;z-index:4;margin:-20px -20px 18px;padding:12px 20px;background:var(--red);color:#fff;font-weight:800}.topalert.show{display:block}.topalert.orange{background:#b26b12}.local-lost{display:none;position:fixed;inset:0;z-index:99;background:rgba(5,7,10,.94);align-items:center;justify-content:center;text-align:center;padding:24px}.local-lost.show{display:flex}.lost-box{max-width:560px;border:1px solid var(--red);border-radius:18px;background:#160d0f;padding:32px;box-shadow:0 20px 80px rgba(0,0,0,.55)}.lost-box h2{font-size:32px;margin:0 0 8px}.lost-box p{color:#f4c9c9;margin:0;font-size:16px}.matrix-wrap{overflow:auto;border:1px solid var(--line);border-radius:14px;background:#0e1318}.matrix{border-collapse:separate;border-spacing:0;min-width:700px;width:100%}.matrix th,.matrix td{border-right:1px solid var(--line);border-bottom:1px solid var(--line);padding:6px 8px;text-align:center;white-space:nowrap}.matrix th{position:sticky;top:0;background:#151c23;z-index:2}.matrix th:first-child{left:0;z-index:3}.matrix .rowhead{position:sticky;left:0;background:#151c23;text-align:left;z-index:1;min-width:230px}.xcell{cursor:pointer;min-width:48px;height:34px;background:rgba(32,42,51,.75);user-select:none;padding:0}.xcell.on{background:var(--green);box-shadow:inset 0 0 0 1px rgba(255,255,255,.18)}.xcell:active{filter:brightness(1.12)}.send-head{display:grid;gap:5px;min-width:110px}.send-head input{width:100%;padding:5px 6px;border-radius:6px;text-align:center;font-size:12px}.sig{display:inline-block;width:8px;height:8px;border-radius:50%;background:#5c6670;margin-right:8px}.sig.green{background:var(--green)}.sig.orange{background:var(--orange)}.sig.red{background:var(--red)}.meterbank{display:grid;grid-template-columns:repeat(auto-fill,minmax(54px,64px));gap:18px;align-items:end;margin:6px 0 24px}.meter{height:340px;display:grid;grid-template-rows:260px auto auto;justify-items:center;align-items:end}.barbox{position:relative;width:22px;height:260px;background:#06090c;border-left:1px solid #111820;border-right:1px solid #111820;overflow:hidden}.seg{position:absolute;left:0;right:0;height:0;bottom:0;transition:height .02s linear}.seg.green{background:var(--green)}.seg.orange{background:var(--orange)}.seg.red{background:var(--red)}.ticks{position:absolute;inset:0;pointer-events:none}.tick{position:absolute;left:-22px;width:18px;border-top:1px solid rgba(190,198,204,.55);font-size:11px;color:#a5adb4;text-align:right;line-height:1;transform:translateY(-1px);font-variant-numeric:tabular-nums}.tick.major{border-top-color:rgba(230,235,238,.75);color:#cbd2d8}.tick.ref18{border-top-color:rgba(32,195,99,.95);color:#bff4d0}.tick.ref10{border-top-color:rgba(242,162,58,.95);color:#ffd38d}.meterlabel{text-align:center;color:#cfd6dc;font-size:12px;line-height:1.12;min-height:28px;max-width:70px;overflow:hidden}.db{text-align:center;font-variant-numeric:tabular-nums;font-size:11px;color:#aeb7bf;min-height:16px;margin-top:6px}.formgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:14px}.field{display:grid;gap:6px;margin-bottom:10px}.note{color:var(--muted)}.empty-note{color:var(--muted);padding:16px 0 22px;font-size:14px}.warn{color:var(--orange)}a{color:var(--blue)}
</style>
</head>
<body>
<div id="localLost" class="local-lost"><div class="lost-box"><h2>Not connected</h2><p>The browser has lost its control connection to this AudioLink device. Check that audiolinkd is still running, then refresh when it is back online.</p></div></div>
<header><div><h1>AudioLink Control</h1><div class="node" id="nodeLine">Starting…</div></div><div class="status"><span id="peerLamp" class="lamp"></span><span id="peerText">No connected device</span></div></header>
<nav><button class="tab active" data-page="home">Home</button><button class="tab" data-page="txrouting">Send Routing</button><button class="tab" data-page="rxrouting">Receive Routing</button><button class="tab" data-page="meters">Peak Meters</button><button class="tab" data-page="setup">Setup</button></nav>
<main>
<div id="topAlert" class="topalert">REMOTE DEVICE OFFLINE</div>
<div id="remoteBanner" class="remotealert">Remote connected device offline.</div>
<section id="home" class="page active"><div class="grid"><div class="card"><h2>Local Link</h2><div class="kv" id="localKv"></div></div><div class="card"><h2>Connected Device</h2><div class="kv" id="peerKv"></div></div><div class="card"><h2>Connection Stats</h2><div class="kv" id="statsKv"></div></div><div class="card"><h2>Audio Engine</h2><div class="kv" id="audioKv"></div></div></div></section>
<section id="txrouting" class="page"><div class="card"><h2>Send Routing</h2><div id="txMatrix"></div></div></section>
<section id="rxrouting" class="page"><div class="card"><h2>Receive Routing</h2><div id="rxMatrix"></div></div></section>
<section id="meters" class="page"><div class="card"><h2>Peak Meters</h2><h3>Send</h3><div class="meterbank" id="txMeters"></div><h3>Receive</h3><div class="meterbank" id="rxMeters"></div><h3>Local Output</h3><div class="meterbank" id="monMeters"></div></div></section>
<section id="setup" class="page"><div class="formgrid"><div class="card"><h2>Link Setup</h2><div class="field"><label>Remote device name</label><input id="cfgRemoteName" autocomplete="off" placeholder="debian-vm-2"></div><div class="field"><label>Remote host IP (leave blank to wait for incoming)</label><input id="cfgPeer" autocomplete="off" placeholder="192.168.64.3 — blank = responder mode"></div><div class="field"><label>This device name</label><input id="cfgNode" autocomplete="off"></div><div class="field"><label>Link password (optional)</label><div style="display:flex;gap:6px"><input id="cfgLinkPw" autocomplete="off" type="password" placeholder="Leave blank if not required" style="flex:1"><button type="button" onclick="togglePwVis()" id="cfgLinkPwBtn" title="Show/hide password" style="padding:0 10px;font-size:1.1em">👁</button></div></div><div class="field"><label>Link token override (advanced — leave blank)</label><input id="cfgToken" autocomplete="off" spellcheck="false" placeholder="Derived automatically from device name"></div><div class="field"><label>Rendezvous server (optional — for internet connections)</label><input id="cfgRendezvous" autocomplete="off" placeholder="https://audiolink.amsound.co.uk"></div><div class="field"><label>Network send channels</label><select id="cfgChannels"><option>1</option><option>2</option><option>4</option><option>6</option><option>8</option><option>16</option><option>24</option><option>32</option><option>40</option><option>64</option></select></div><div id="setupLink" class="kv"></div></div><div class="card"><h2>Codec</h2><div class="field"><label>Codec</label><select disabled><option>Opus</option></select></div><div class="field"><label>Bitrate per channel</label><select id="bitrate"><option value="32000">32 kb/s</option><option value="48000">48 kb/s</option><option value="64000">64 kb/s</option><option value="96000">96 kb/s</option><option value="128000">128 kb/s</option><option value="192000">192 kb/s</option><option value="256000">256 kb/s</option></select></div><div class="field"><label>Frame size</label><select disabled><option>20 ms</option></select></div><div class="field"><label>Incoming audio buffer</label><select id="rxBuffer"><option value="5">5 ms</option><option value="10">10 ms</option><option value="20">20 ms</option><option value="40">40 ms</option><option value="60">60 ms</option><option value="80">80 ms</option><option value="100">100 ms</option><option value="120">120 ms</option><option value="140">140 ms</option><option value="160">160 ms</option><option value="180">180 ms</option><option value="200">200 ms</option><option value="250">250 ms</option><option value="300">300 ms</option><option value="400">400 ms</option><option value="500">500 ms</option><option value="750">750 ms</option><option value="1000">1 s</option><option value="1500">1.5 s</option><option value="2000">2 s</option><option value="3000">3 s</option><option value="5000">5 s</option><option value="10000">10 s</option></select></div><div class="field"><button onclick="applySetup()">Apply and rebuild engine</button></div><pre id="restartCommand" class="cmd"></pre></div><div class="card"><h2>Audio Devices</h2><div id="devices"></div></div></div></section>
</main>
<script>
let status={}, stats={}, matrix={sources:[],destinations:[],routes:[]}, devices={};
let matrixRenderKey='', routeBusy=false, localOk=true;
let cfgDirty={peer:false,node:false,token:false,channels:false,bitrate:false,rxBuffer:false};
function markCfgDirty(id,key){let el=$(id); if(el) el.addEventListener('input',()=>cfgDirty[key]=true); if(el) el.addEventListener('change',()=>cfgDirty[key]=true);} 
window.addEventListener('DOMContentLoaded',()=>{markCfgDirty('cfgRemoteName','remoteName');markCfgDirty('cfgPeer','peer');markCfgDirty('cfgNode','node');markCfgDirty('cfgLinkPw','linkPw');markCfgDirty('cfgToken','token');markCfgDirty('cfgRendezvous','rendezvous');markCfgDirty('cfgChannels','channels');markCfgDirty('bitrate','bitrate');markCfgDirty('rxBuffer','rxBuffer');});
const $=id=>document.getElementById(id);
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{document.querySelectorAll('.tab,.page').forEach(x=>x.classList.remove('active'));b.classList.add('active');$(b.dataset.page).classList.add('active')});
function setLocalOk(ok){localOk=ok;$('localLost').className='local-lost '+(ok?'':'show')}
function lampClass(db){if(db>-10)return'red';if(db>-18)return'orange';if(db>=-90)return'green';return''}
function setKv(id,pairs){$(id).innerHTML=pairs.map(([k,v])=>`<span>${k}</span><b>${v}</b>`).join('')}
function pct(db){db=Math.max(-60,Math.min(0,db));return 100-((db+60)/60*100)}
function meterColour(db){if(db>-10)return'red';if(db>-18)return'orange';return'green'}
function segHeights(db){
  if(!Number.isFinite(db)||db<=-100)return{g:0,o:0,r:0};
  let shown=Math.max(-60,Math.min(0,db));
  let g=Math.max(0,Math.min(shown,-18)-(-60))/60*100;
  let o=Math.max(0,Math.min(shown,-10)-(-18))/60*100;
  let r=Math.max(0,shown-(-10))/60*100;
  return{g,o,r};
}
function meter(label,db,key,sublabel){let finite=Number.isFinite(db)&&db>-100;let shown=Math.max(-60,Math.min(0,finite?db:-120));let ticks=[0,-10,-18,-60].map(t=>`<div class="tick ${t===0||t===-60?'major':t===-18?'ref18':'ref10'}" style="top:${pct(t)}%">${t}</div>`).join('');let h=segHeights(db);let sub=sublabel?`<br><span style="font-size:10px;color:var(--muted);display:block;margin-top:2px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:64px">${sublabel}</span>`:'';return `<div class="meter"><div class="barbox"><div class="seg green" style="height:${h.g}%"></div><div class="seg orange" style="bottom:${h.g}%;height:${h.o}%"></div><div class="seg red" style="bottom:${h.g+h.o}%;height:${h.r}%"></div><div class="ticks">${ticks}</div></div><div class="db">${finite?shown.toFixed(1):'−∞'} dBFS</div><div class="meterlabel">${label}${sub}</div></div>`}
function routeSet(){return new Set((matrix.routes||[]).map(r=>r.source+'>'+r.destination))}
function jsq(v){return JSON.stringify(v).replace(/</g,'\u003c')}
async function toggleRoute(src,dst){if(routeBusy)return;routeBusy=true;try{let routes=[...(matrix.routes||[])];let key=src+'>'+dst;let i=routes.findIndex(r=>r.source+'>'+r.destination===key);if(i>=0)routes.splice(i,1);else routes.push({source:src,destination:dst});let body={routes};if(dst.startsWith('output:'))body.monitor_mode='patch_matrix';let res=await fetch('/api/routes',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});matrix=await res.json();matrixRenderKey='';renderMatrices()}finally{routeBusy=false}}
function matrixShapeKey(){return JSON.stringify({s:(matrix.sources||[]).map(s=>[s.id,s.label,s.kind]),d:(matrix.destinations||[]).map(d=>[d.id,d.label,d.kind]),r:(matrix.routes||[]).map(r=>[r.source,r.destination]).sort()})}
function sourceDb(s){let m=s.id.match(/ch:(\d+)/);if(s.kind==='network_receive'&&m){let ch=parseInt(m[1]);return (stats.rx_peak_dbfs||[])[ch]??-120}if(s.kind==='test_tone'){if(s.id==='ebu:r')return -18;if(s.id==='ebu:l'){let pos=(Date.now()%3000);return pos<250?-120:-18;}return -120}if(s.kind==='physical_input'){let m=s.id.match(/^input:(\d+)$/);if(m){let ch=parseInt(m[1]);return (stats.input_peak_dbfs||[])[ch]??-120}}return -120}
function updateSignalLamps(){document.querySelectorAll('[data-src-id]').forEach(el=>{let s=(matrix.sources||[]).find(x=>x.id===el.dataset.srcId);if(!s)return;el.className='sig '+lampClass(sourceDb(s));})}
function sendChannelIndex(id){let m=id.match(/^stream:0:ch:(\d+)$/);return m?parseInt(m[1]):null}
async function sendLocalLabels(){let txDests=(matrix.destinations||[]).filter(d=>d.kind==='network_send');let labels=txDests.map(d=>{let idx=sendChannelIndex(d.id);let el=idx==null?null:document.querySelector(`[data-label-input="${idx}"]`);return el?el.value:d.label.replace(/^Send \d+ — /,'')});try{let res=await fetch('/api/local-labels',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({labels})});matrix=await res.json();matrixRenderKey='';renderMatrices()}catch(e){console.error(e)}}
function destHeader(d){let idx=sendChannelIndex(d.id);if(idx==null)return d.label;let current=d.label.replace(/^Send \d+ — /,'');return `<div class="send-head"><div>Send ${idx+1}</div><input data-label-input="${idx}" value="${current.replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;')}" onchange="sendLocalLabels()" onkeydown="if(event.key==='Enter')this.blur()" onclick="event.stopPropagation()"></div>`}
function kindLabel(k){return{network_send:'Send',network_receive:'Receive',physical_input:'Local Input',physical_output:'Local Output',test_tone:'Test Tone'}[k]||k.replaceAll('_',' ')}
function matrixTable(title,sources,destinations,emptyText){if(!sources.length||!destinations.length)return `<div class="empty-note">${emptyText}</div>`;let set=routeSet();let html='<div class="matrix-wrap"><table class="matrix"><thead><tr><th></th>'+destinations.map(d=>`<th>${destHeader(d)}</th>`).join('')+'</tr></thead><tbody>';sources.forEach(s=>{html+=`<tr><th class="rowhead"><span data-src-id="${s.id}" class="sig ${lampClass(sourceDb(s))}"></span>${s.label}<br><small>${kindLabel(s.kind)}</small></th>`;destinations.forEach(d=>{let on=set.has(s.id+'>'+d.id);html+=`<td class="xcell ${on?'on':''}" onclick='toggleRoute(${jsq(s.id)},${jsq(d.id)})' title="${s.label} → ${d.label}"></td>`});html+='</tr>'});return html+'</tbody></table></div>'}
function renderMatrices(){let key=matrixShapeKey();if(key===matrixRenderKey){updateSignalLamps();return}matrixRenderKey=key;let sources=matrix.sources||[], destinations=matrix.destinations||[];let txSources=sources.filter(s=>['test_tone','physical_input'].includes(s.kind));let txDests=destinations.filter(d=>d.kind==='network_send');let rxSources=sources.filter(s=>s.kind==='network_receive');let rxDests=destinations.filter(d=>d.kind==='physical_output');$('txMatrix').innerHTML=matrixTable('Send Routing',txSources,txDests,'No send channels');$('rxMatrix').innerHTML=matrixTable('Receive Routing',rxSources,rxDests,'No receive sources')}
function setRemoteAlert(st){let alert=$('topAlert'), banner=$('remoteBanner');let isGood=st==='green';let msg=st==='orange'?'REMOTE DEVICE DEGRADED':'REMOTE DEVICE OFFLINE';alert.className='topalert '+(isGood?'':'show ')+(st==='orange'?'orange':'');alert.textContent=msg;banner.className='remotealert';banner.textContent=''}
function setSelectValue(id,value){let el=$(id);if(!el)return;let v=String(value);if([...el.options].some(o=>o.value===v||o.text===v))el.value=v}
function shellQuote(v){return "'"+String(v).replace(/'/g,"'\''")+"'"}
function showRestartCommand(){let peer=$('cfgPeer').value||status.runtime?.remote_host||'';let node=$('cfgNode').value||status.node_id||'';let remoteName=$('cfgRemoteName').value||status.runtime?.remote_device_name||'';let channels=$('cfgChannels').value||status.local_channels||2;let bitrate=$('bitrate').value||status.runtime?.opus_bitrate_per_channel||128000;let rxBuffer=$('rxBuffer').value||status.runtime?.latency_ms||120;let cmd=`audiolinkd bidir --remote-name ${shellQuote(remoteName)}${peer?' --remote-host '+shellQuote(peer.split(':')[0]):''}  --channels ${channels} --id ${shellQuote(node)} --bitrate ${bitrate} --latency-ms ${rxBuffer}`;$('restartCommand').textContent=cmd}
async function applySetup(){let body={remote:$('cfgPeer').value,remote_device_name:$('cfgRemoteName').value,link_password:$('cfgLinkPw').value||undefined,node_id:$('cfgNode').value,token:$('cfgToken').value||undefined,channels:Number($('cfgChannels').value),opus_bitrate_per_channel:Number($('bitrate').value),receive_buffer_ms:Number($('rxBuffer').value),rendezvous_url:$('cfgRendezvous').value||undefined};$('restartCommand').textContent='Applying…';try{let res=await fetch('/api/setup/apply',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});let txt=await res.text();if(!res.ok){$('restartCommand').textContent=txt;return}cfgDirty={peer:false,node:false,remoteName:false,linkPw:false,token:false,channels:false,bitrate:false,rxBuffer:false,rendezvous:false};$('restartCommand').textContent='Engine rebuilding — reconnecting shortly.'}catch(e){$('restartCommand').textContent='Apply failed: '+e}}
function togglePwVis(){let f=$('cfgLinkPw');f.type=f.type==='password'?'text':'password';}
function render(){setLocalOk(true);let dev=status.remote||{};let st=status.peer_status||'gray';let conflict=status.remote_conflict;$('peerLamp').className='lamp '+(st==='green'?'green':st==='orange'?'orange':'');let connText=st==='green'?('Connected — auto-handshake established'+(dev.node_id?' with '+dev.node_id:'')):st==='orange'?'Remote device degraded':'Remote device offline';$('peerText').textContent=connText;if(conflict){$('peerText').textContent='Connection conflict — '+conflict+' attempted to connect while already connected to '+(dev.node_id||'another device')}setRemoteAlert(st);$('nodeLine').textContent=`Node ${status.node_id||''} · ${status.monitor_mode||''}`;setKv('localKv',[['Device Name',status.node_id||'—'],['Mode','Bidirectional'],['Source',status.runtime?.source||'—'],['Network send channels',status.local_channels??'—'],['Opus bitrate',status.runtime?.opus_bitrate_per_channel?Math.round(status.runtime.opus_bitrate_per_channel/1000)+' kb/s':'—'],['Local inputs',status.local_input_channels??0],['Monitor',status.monitor_mode||'—']]);setKv('peerKv',[['Remote device name',dev.node_id||status.runtime?.remote_device_name||'—'],['Remote host',status.runtime?.remote_host||'—'],['RX channels',status.remote_channels??0],['Status',status.peer_status||'gray'],['Last keepalive',status.last_control_age_ms!=null?Math.round(status.last_control_age_ms/100)/10+' s ago':'—'],['Last audio',status.last_audio_age_ms!=null?Math.round(status.last_audio_age_ms/100)/10+' s ago':'—'],['Metadata',dev.labels?dev.labels.join(', '):'—']]);setKv('statsKv',[['TX rate',((stats.tx_mbps??0)).toFixed(3)+' Mb/s'],['RX rate',((stats.rx_mbps??0)).toFixed(3)+' Mb/s'],['Packet loss',((stats.loss_percent??0)).toFixed(3)+' %'],['Missing packets',stats.seq_missing??0],['Jitter',((stats.jitter_ms??0)).toFixed(2)+' ms'],['RTT',stats.rtt_ms?(stats.rtt_ms.toFixed(1)+' ms'):'not measured yet'],['Estimated one-way latency',stats.one_way_latency_ms?(stats.one_way_latency_ms.toFixed(1)+' ms'):'pending'],['Incoming audio buffer',(stats.fill_ms??0)+' ms'],['Configured buffer',(status.runtime?.latency_ms??'—')+' ms'],['Effective target buffer',(stats.target_ms??status.runtime?.effective_latency_ms??'—')+' ms'],['Estimated audio latency',stats.one_way_latency_ms&&stats.fill_ms?(((stats.one_way_latency_ms)+(stats.fill_ms??0)).toFixed(1)+' ms'):'pending'],['Drift pressure',(stats.drift_pressure_ppm??0)+' ppm'],['Decoded fps',(stats.decoded_fps??0).toFixed(1)],['TX fps',(stats.tx_fps??0).toFixed(1)],['Queued groups',stats.queued_groups??0],['Output underflows',stats.output_underflows??0],['PLC channels',stats.plc_channels??0],['Ring overflows',stats.ring_overflows??0]]);setKv('audioKv',[['Codec','Opus'],['Bitrate',status.runtime?.opus_bitrate_per_channel?Math.round(status.runtime.opus_bitrate_per_channel/1000)+' kb/s per channel':'—'],['Frame','20 ms / 960 samples'],['Incoming buffer',(status.runtime?.latency_ms??'—')+' ms configured / '+(status.runtime?.effective_latency_ms??stats.target_ms??'—')+' ms effective'],['Phase lock',stats.phase_lock?'on':'off'],['Generator','EBU R49']]);if(!cfgDirty.remoteName)$('cfgRemoteName').value=status.runtime?.remote_device_name||'';if(!cfgDirty.peer)$('cfgPeer').value=(status.runtime?.remote_host||'').split(':')[0];if(!cfgDirty.node)$('cfgNode').value=status.node_id||'';if(!cfgDirty.token)$('cfgToken').value='';if(!cfgDirty.rendezvous)$('cfgRendezvous').value=status.runtime?.rendezvous_url||'';if(!cfgDirty.linkPw){$('cfgLinkPw').value='';$('cfgLinkPw').placeholder=status.runtime?.link_password_configured?'Password is set — enter new password to change, or leave blank to keep':'Leave blank if not required';}if(!cfgDirty.channels)setSelectValue('cfgChannels',status.local_channels??2);if(!cfgDirty.bitrate)setSelectValue('bitrate',status.runtime?.opus_bitrate_per_channel??128000);if(!cfgDirty.rxBuffer)setSelectValue('rxBuffer',status.runtime?.latency_ms??120);setKv('setupLink',[['Remote device name',status.runtime?.remote_device_name||'not configured'],['Remote host (initiator only)',status.runtime?.remote_host||'blank (responder mode)'],['Rendezvous server',status.runtime?.rendezvous_url||'not configured'],['Current device name',status.node_id||'—'],['Incoming buffer',(status.runtime?.latency_ms??'—')+' ms configured'],['Effective buffer',(status.runtime?.effective_latency_ms??stats.target_ms??'—')+' ms'],['Config file','audiolinkd_config.json']]);$('devices').innerHTML=`<div class="kv"><span>Sample rate</span><b>${devices.sample_rate||48000} Hz</b><span>Default input</span><b>${devices.default_input||'none'}</b><span>Input channels</span><b>${devices.default_input_channels??0}</b><span>Default output</span><b>${devices.default_output||'none'}</b><span>Output channels</span><b>${devices.default_output_channels??0}</b></div><h3>Inputs</h3><p>${(devices.inputs||[]).join('<br>')||'none'}</p><h3>Outputs</h3><p>${(devices.outputs||[]).join('<br>')||'none'}</p>`;let txLabels=(matrix.destinations||[]).filter(d=>d.kind==='network_send').map(d=>d.label.replace(/^Send \d+ — /,''));let rxLabels=(matrix.sources||[]).filter(s=>s.kind==='network_receive').map(s=>s.label.replace(/^\S+ /,''));$('txMeters').innerHTML=(stats.tx_peak_dbfs||[]).map((v,i)=>meter('Send '+(i+1),v,'tx'+i,txLabels[i]||'')).join('')||'<p class="note">No send channels</p>';let remoteOnline=(st==='green'||st==='orange')&&(status.remote_channels||0)>0;$('rxMeters').innerHTML=remoteOnline?(stats.rx_peak_dbfs||[]).map((v,i)=>meter('Receive '+(i+1),v,'rx'+i,rxLabels[i]||'')).join(''):'<div class="empty-note">No connected remote device</div>';$('monMeters').innerHTML=(stats.monitor_peak_dbfs||[-120,-120]).map((v,i)=>meter(i?'Local Output 2':'Local Output 1',v,'mon'+i)).join('');renderMatrices()}
async function loadDevices(){try{devices=await fetch('/api/audio/devices').then(r=>r.json())}catch(e){console.error(e)}}
async function poll(){try{[status,stats,matrix]=await Promise.all([fetch('/api/status').then(r=>r.json()),fetch('/api/stats').then(r=>r.json()),fetch('/api/routes').then(r=>r.json())]);render()}catch(e){console.error(e);setLocalOk(false)}}
loadDevices().then(poll);setInterval(poll,100);
</script>
</body></html>"##.to_string())
}

async fn status_handler(State(state): State<WebState>) -> Json<StatusResponse> {
    let now_ms = now_millis();
    let last_control = state.last_control_ms.load(Ordering::Relaxed);
    let last_audio = state.last_audio_ms.load(Ordering::Relaxed);
    let remote_conflict = state.remote_conflict.lock().ok().and_then(|c| c.clone());
    Json(StatusResponse {
        node_id: state.node_id.clone(),
        uptime_seconds: state.started_at.elapsed().as_secs(),
        peer_status: state.peer_status(),
        monitor_mode: state.monitor_mode(),
        local_channels: state.local_channels,
        local_input_channels: state.local_input_channels,
        remote_channels: state.remote_channels.load(Ordering::Relaxed),
        send_enabled: state.send_enabled,
        recv_enabled: state.recv_enabled,
        remote: state.remote_metadata.lock().ok().and_then(|m| m.clone()),
        runtime: state.runtime.clone(),
        last_control_age_ms: if last_control == 0 { 0 } else { now_ms.saturating_sub(last_control) },
        last_audio_age_ms: if last_audio == 0 { 0 } else { now_ms.saturating_sub(last_audio) },
        remote_conflict,
    })
}

async fn routes_get_handler(State(state): State<WebState>) -> Json<MatrixResponse> {
    Json(matrix_for_state(&state))
}

async fn routes_post_handler(State(state): State<WebState>, Json(req): Json<RoutesRequest>) -> impl IntoResponse {
    if req.routes.iter().any(|r| r.source.is_empty() || r.destination.is_empty()) {
        return (StatusCode::BAD_REQUEST, "routes require non-empty source and destination").into_response();
    }
    if let Some(mode) = req.monitor_mode {
        state.monitor_mode.store(mode.as_u8(), Ordering::Relaxed);
    }
    apply_routes_to_masks(&req.routes, &state.output_route_masks);
    apply_routes_to_tx_sources(&req.routes, &state.tx_tone_source_for_send);
    if let Ok(mut routes) = state.routes.lock() {
        *routes = req.routes.clone();
        save_persisted_routes(&routes);
    }
    Json(matrix_for_state(&state)).into_response()
}

async fn local_labels_post_handler(State(state): State<WebState>, Json(req): Json<LocalLabelsRequest>) -> impl IntoResponse {
    let mut labels: Vec<String> = req
        .labels
        .into_iter()
        .take(state.local_channels)
        .enumerate()
        .map(|(idx, label)| {
            let trimmed = label.trim();
            if trimmed.is_empty() {
                local_channel_label(idx)
            } else {
                trimmed.chars().take(48).collect()
            }
        })
        .collect();
    while labels.len() < state.local_channels {
        labels.push(local_channel_label(labels.len()));
    }

    if let Ok(mut current) = state.local_labels.lock() {
        *current = labels.clone();
    }

    // Persist labels so they survive engine restarts and channel count changes.
    let mut persisted = load_persisted_state();
    persisted.config.channel_labels = Some(labels.clone());
    save_persisted_state(&persisted);

    if state.handshake_connected.load(Ordering::Relaxed) {
        let pkt = build_metadata_packet_with_labels(
            &state.device_name_token,
            state.local_channels,
            &state.node_id,
            &labels,
        );
        if let Err(e) = state.metadata_socket.send(&pkt) {
            tracing::warn!("Metadata resend after label edit failed: {e}");
        } else {
            tracing::info!("Metadata: resent 09 0a after local channel label edit");
        }
    }

    Json(matrix_for_state(&state)).into_response()
}

async fn preset_save_handler(State(state): State<WebState>, Json(req): Json<PresetRequest>) -> impl IntoResponse {
    let routes = state.routes.lock().map(|r| r.clone()).unwrap_or_default();
    if let Ok(mut presets) = state.presets.lock() {
        presets.insert(req.name.clone(), routes);
    }
    (StatusCode::OK, format!("saved preset '{}'", req.name))
}

async fn preset_recall_handler(State(state): State<WebState>, Json(req): Json<PresetRequest>) -> impl IntoResponse {
    let routes = state.presets.lock().ok().and_then(|p| p.get(&req.name).cloned());
    match routes {
        Some(routes) => {
            apply_routes_to_masks(&routes, &state.output_route_masks);
            apply_routes_to_tx_sources(&routes, &state.tx_tone_source_for_send);
            if let Ok(mut current) = state.routes.lock() { *current = routes.clone(); save_persisted_routes(&current); }
            Json(matrix_for_state(&state)).into_response()
        }
        None => (StatusCode::NOT_FOUND, format!("preset '{}' not found", req.name)).into_response(),
    }
}


async fn setup_apply_handler(State(state): State<WebState>, Json(req): Json<SetupApplyRequest>) -> impl IntoResponse {
    let remote = req.remote.trim();
    let remote_device_name = req.remote_device_name.trim();
    let node_id = req.node_id.trim();
    if remote_device_name.is_empty() {
        return (StatusCode::BAD_REQUEST, "Remote device name is required").into_response();
    }
    if node_id.is_empty() {
        return (StatusCode::BAD_REQUEST, "Device name is required").into_response();
    }
    if req.channels == 0 || req.channels > MAX_CHANNELS {
        return (StatusCode::BAD_REQUEST, format!("Network send channels must be 1-{MAX_CHANNELS}")).into_response();
    }

    // Derive link token from remote device name + optional password.
    // If an explicit hex token is provided it overrides derivation (power-user escape hatch).
    let link_password = req.link_password.as_deref().unwrap_or("").trim();
    let (token_hex, token_derived) = if let Some(explicit) = req.token.as_deref().filter(|t| !t.trim().is_empty()) {
        match parse_token_arg(explicit.trim()) {
            Ok(token) => (token_to_hex(&token), false),
            Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid explicit token: {e}")).into_response(),
        }
    } else {
        let token = derive_link_token(node_id, remote_device_name, if link_password.is_empty() { None } else { Some(link_password) });
        (token_to_hex(&token), true)
    };

    if !(8_000..=512_000).contains(&req.opus_bitrate_per_channel) {
        return (StatusCode::BAD_REQUEST, "Opus bitrate must be 8000-512000 bits/sec per channel").into_response();
    }
    let receive_buffer_ms = req.receive_buffer_ms.clamp(MIN_LATENCY_MS, MAX_LATENCY_MS);
    let effective_buffer_ms = effective_receive_buffer_ms(receive_buffer_ms);

    let _guard = match state.restart_lock.try_lock() {
        Ok(g) => g,
        Err(_) => return (StatusCode::CONFLICT, "Engine rebuild is already in progress").into_response(),
    };

    let mut args = vec![
        "bidir".to_string(),
        "--remote-name".to_string(),
        remote_device_name.to_string(),
        "--channels".to_string(),
        req.channels.to_string(),
        "--id".to_string(),
        node_id.to_string(),
        "--token".to_string(),
        token_hex.clone(),
        "--bitrate".to_string(),
        req.opus_bitrate_per_channel.to_string(),
        "--latency-ms".to_string(),
        receive_buffer_ms.to_string(),
        "--monitor".to_string(),
        "matrix".to_string(),
    ];
    // Remote host is optional — blank means responder mode.
    if !remote.is_empty() {
        args.push("--remote-host".to_string());
        args.push(split_host_port(remote));
    }
    if !link_password.is_empty() {
        args.push("--link-password".to_string());
        args.push(link_password.to_string());
    }
    if let Some(url) = req.rendezvous_url.as_deref().filter(|u| !u.trim().is_empty()) {
        args.push("--rendezvous".to_string());
        args.push(url.to_string());
    }
    if state.runtime.fixed_jitter { args.push("--fixed-jitter".to_string()); }
    if !state.runtime.phase_lock { args.push("--no-phase-lock".to_string()); }
    if !state.runtime.send_enabled { args.push("--no-send".to_string()); }
    if !state.runtime.recv_enabled { args.push("--no-recv".to_string()); }

    let exe = match std::env::current_exe() {
        Ok(p) => p,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, format!("Cannot locate executable: {e}")).into_response(),
    };
    let mut command_preview = Vec::with_capacity(args.len() + 1);
    command_preview.push(exe.display().to_string());
    let mut hide_next = false;
    for arg in &args {
        if hide_next {
            command_preview.push("<token hidden>".to_string());
            hide_next = false;
        } else {
            if arg == "--token" || arg == "--link-password" { hide_next = true; }
            command_preview.push(arg.clone());
        }
    }

    let token_source = if token_derived {
        if link_password.is_empty() {
            format!("derived from {node_id}:{remote_device_name} (sorted pair)")
        } else {
            format!("derived from {node_id}:{remote_device_name} + password (sorted pair)")
        }
    } else {
        "explicit override".to_string()
    };

    save_persisted_config(PersistedRuntimeConfig {
        remote: if remote.is_empty() { None } else { Some(split_host_port(remote)) },
        remote_device_name: Some(remote_device_name.to_string()),
        link_password: if link_password.is_empty() { None } else { Some(link_password.to_string()) },
        node_id: Some(node_id.to_string()),
        token_hex: Some(token_hex.clone()),
        channels: Some(req.channels),
        opus_bitrate_per_channel: Some(req.opus_bitrate_per_channel),
        latency_ms: Some(receive_buffer_ms),
        fixed_jitter: Some(state.runtime.fixed_jitter),
        phase_lock: Some(state.runtime.phase_lock),
        channel_labels: None, // preserved by save_persisted_config — not overwritten here
        rendezvous_url: req.rendezvous_url.clone().filter(|u| !u.trim().is_empty()),
    });

    let role = if remote.is_empty() { "responder (waiting for incoming)" } else { "initiator" };
    tracing::warn!(
        "Setup Apply: role={role} remote_device={remote_device_name} remote_host={} node={} \
         channels={} bitrate={} receive_buffer={}ms effective_buffer={}ms token={} saved audiolinkd_config.json",
        if remote.is_empty() { "<blank>" } else { remote },
        node_id, req.channels, req.opus_bitrate_per_channel, receive_buffer_ms, effective_buffer_ms,
        token_source
    );

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(250));
        #[cfg(unix)]
        {
            let err = Command::new(&exe).args(&args).exec();
            eprintln!("exec failed: {err}");
            std::process::exit(1);
        }
        #[cfg(not(unix))]
        {
            match Command::new(&exe).args(&args).spawn() {
                Ok(_) => std::process::exit(0),
                Err(e) => {
                    eprintln!("engine rebuild spawn failed: {e}");
                    std::process::exit(1);
                }
            }
        }
    });

    Json(SetupApplyResponse { status: "rebuilding".to_string(), command: command_preview }).into_response()
}

async fn stats_handler(State(state): State<WebState>) -> Json<UiStats> {
    let mut s = state.stats.lock().map(|s| s.clone()).unwrap_or_default();
    let rx_n = state.remote_channels.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
    s.tx_peak_dbfs = state.meters.snapshot_tx(state.local_channels);
    s.input_peak_dbfs = state.meters.snapshot_input(state.local_input_channels);
    s.rx_peak_dbfs = state.meters.snapshot_rx(rx_n);
    s.monitor_peak_dbfs = state.meters.snapshot_monitor();
    Json(s)
}

async fn peers_handler(State(state): State<WebState>) -> Json<serde_json::Value> {
    let metadata = state.remote_metadata.lock().ok().and_then(|m| m.clone());
    Json(serde_json::json!([{ "id": "remote", "status": state.peer_status(), "metadata": metadata }]))
}

async fn streams_handler(State(state): State<WebState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "outgoing": [{ "id": "stream:0", "name": format!("{} Send", state.node_id), "channels": state.local_channels }],
        "incoming": state.remote_metadata.lock().ok().and_then(|m| m.clone()),
    }))
}

async fn devices_handler(State(state): State<WebState>) -> Json<DeviceResponse> {
    Json((*state.devices).clone())
}

async fn remotestatus_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": 0 }))
}

async fn events_handler(ws: WebSocketUpgrade, State(state): State<WebState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| events_socket(socket, state))
}

fn control_web_state(node_id: String, shared_token: [u8; 16], send_channels: usize, opus_bitrate_per_channel: u32, jitter: JitterConfig) -> Result<WebState> {
    let send_channels = send_channels.clamp(1, MAX_CHANNELS);
    let handshake_connected = Arc::new(AtomicBool::new(false));
    let last_control_ms = Arc::new(AtomicU64::new(0));
    let last_audio_ms = Arc::new(AtomicU64::new(0));
    let remote_channels = Arc::new(AtomicUsize::new(0));
    let remote_metadata = Arc::new(Mutex::new(None));
    let monitor_mode_atomic = Arc::new(AtomicU8::new(MonitorMode::PatchMatrix.as_u8()));
    let output_route_masks = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    let tx_tone_source_for_send = Arc::new((0..MAX_CHANNELS).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let local_labels = Arc::new(Mutex::new(load_persisted_labels(send_channels)));
    let presets = Arc::new(Mutex::new(HashMap::new()));
    let mut initial_stats = UiStats::default();
    initial_stats.target_ms = jitter.target_delay_ms;
    let stats = Arc::new(Mutex::new(initial_stats));
    let meters = Arc::new(MeterBank::new());
    let devices = Arc::new(scan_audio_devices_once());
    let local_input_channels = if devices.default_input_channels == 0 { 0 } else { devices.default_input_channels.min(MAX_CHANNELS) };
    let initial_routes = load_persisted_routes(local_input_channels, send_channels);
    apply_routes_to_masks(&initial_routes, &output_route_masks);
    apply_routes_to_tx_sources(&initial_routes, &tx_tone_source_for_send);
    let routes = Arc::new(Mutex::new(initial_routes));
    let metadata_socket = Arc::new(UdpSocket::bind("0.0.0.0:0")?);
    let state = WebState {
        started_at: Instant::now(),
        node_id: node_id.clone(),
        local_channels: send_channels,
        local_input_channels,
        send_enabled: true,
        recv_enabled: true,
        handshake_connected,
        last_control_ms,
        last_audio_ms,
        remote_channels,
        remote_metadata,
        monitor_mode: monitor_mode_atomic,
        output_route_masks,
        tx_tone_source_for_send,
        local_labels,
        metadata_socket,
        device_name_token: derive_token_from_text(&node_id),
        routes,
        presets,
        stats,
        meters,
        runtime: RuntimeSummary {
            mode: "bidirectional-ready".into(),
            remote_host: String::new(),
            remote_device_name: String::new(),
            source: "Matrix".into(),
            codec: "Opus".into(),
            opus_bitrate_per_channel,
            frame_ms: 20,
            tx_channels: send_channels,
            token_configured: false,
            token_hint: token_to_hex(&shared_token),
            token_hex: token_to_hex(&shared_token),
            send_enabled: true,
            recv_enabled: true,
            latency_ms: jitter.configured_delay_ms,
            effective_latency_ms: jitter.target_delay_ms,
            fixed_jitter: !jitter.adaptive,
            phase_lock: jitter.phase_lock,
            web_note: "Web control is running. Configure a remote device and apply to start the audio/network engine.".into(),
            link_password_configured: false,
            rendezvous_url: String::new(),
        },
        devices,
        restart_lock: Arc::new(Mutex::new(())),
        rtt_us10: Arc::new(AtomicU32::new(0)),
        remote_conflict: Arc::new(Mutex::new(None)),
    };
    spawn_control_only_metering(&state);
    Ok(state)
}


fn spawn_control_only_metering(state: &WebState) {
    // Web-first mode has no UDP/audio engine yet, but the control surface still
    // needs honest local TX-source metering. This lightweight meter path reads
    // the default input device and renders the explicitly patched transmit
    // matrix into the Network Send meters. It never sends network audio and it
    // never writes monitor output.
    let meters = Arc::clone(&state.meters);
    let tx_source_masks = Arc::clone(&state.tx_tone_source_for_send);
    let ui_stats = Arc::clone(&state.stats);
    let send_channels = state.local_channels.min(MAX_CHANNELS);
    let input_channels = state.local_input_channels.min(MAX_CHANNELS);

    std::thread::spawn(move || {
        let host = cpal::default_host();
        let input_rings: Arc<Mutex<Vec<VecDeque<f32>>>> = Arc::new(Mutex::new(
            (0..input_channels).map(|_| VecDeque::with_capacity(CAP_RING_SIZE)).collect(),
        ));

        let _input_stream: Option<cpal::Stream> = if input_channels > 0 {
            match host.default_input_device() {
                Some(in_device) => {
                    let in_config = cpal::StreamConfig {
                        channels: input_channels as u16,
                        sample_rate: cpal::SampleRate(SAMPLE_RATE),
                        buffer_size: ALSA_PERIOD,
                    };
                    let rings_cb = Arc::clone(&input_rings);
                    let meters_cb = Arc::clone(&meters);
                    let mut peak_acc = vec![0.0f32; input_channels];
                    let mut sample_count: usize = 0;
                    match in_device.build_input_stream(
                        &in_config,
                        move |data: &[f32], _| {
                            if let Ok(mut rings) = rings_cb.lock() {
                                for frame in data.chunks(input_channels) {
                                    for ch in 0..input_channels {
                                        let s = frame.get(ch).copied().unwrap_or(0.0);
                                        if let Some(ring) = rings.get_mut(ch) {
                                            if ring.len() >= CAP_RING_SIZE { ring.pop_front(); }
                                            ring.push_back(s);
                                        }
                                        peak_acc[ch] = peak_acc[ch].max(s.abs());
                                    }
                                    sample_count += 1;
                                    if sample_count >= FRAME_SAMPLES {
                                        for ch in 0..input_channels {
                                            meters_cb.set_input_peak(ch, peak_dbfs_from_peak(peak_acc[ch]));
                                            peak_acc[ch] = 0.0;
                                        }
                                        sample_count = 0;
                                    }
                                }
                            }
                        },
                        |e| tracing::error!("Control input meter error: {e}"),
                        None,
                    ) {
                        Ok(stream) => {
                            if let Err(e) = stream.play() {
                                tracing::warn!("Control input meter could not start: {e}");
                                None
                            } else {
                                tracing::info!("Control input metering active; no network engine is running yet");
                                Some(stream)
                            }
                        }
                        Err(e) => {
                            tracing::warn!("Control input meter unavailable: {e}");
                            None
                        }
                    }
                }
                None => None,
            }
        } else {
            None
        };

        let mut absolute_sample: u64 = 0;
        let frame_dur = Duration::from_micros(20_000);
        let mut next_deadline = Instant::now() + frame_dur;
        let mut frames_total: u64 = 0;
        let mut frames_at_last_stats: u64 = 0;
        let mut last_stats = Instant::now();

        loop {
            let frame_start_sample = absolute_sample;
            let mut input_blocks = vec![vec![0.0f32; FRAME_SAMPLES]; input_channels];
            if input_channels > 0 {
                if let Ok(mut rings) = input_rings.lock() {
                    for ch in 0..input_channels {
                        if let Some(ring) = rings.get_mut(ch) {
                            for i in 0..FRAME_SAMPLES {
                                input_blocks[ch][i] = ring.pop_front().unwrap_or(0.0);
                            }
                        }
                    }
                }
            }

            for ch in 0..send_channels {
                let mask = tx_source_masks
                    .get(ch)
                    .map(|v| v.load(Ordering::Relaxed) as u64)
                    .unwrap_or(0);
                let mut frame = vec![0.0f32; FRAME_SAMPLES];
                let mut active_sources = 0usize;

                for bit in 0..64 {
                    if (mask & (1u64 << bit)) == 0 { continue; }
                    let Some(source_code) = source_code_from_bit_index(bit) else { continue; };
                    match source_code {
                        TX_SRC_EBU_L | TX_SRC_EBU_R => {
                            for (i, s) in frame.iter_mut().enumerate() {
                                *s += tx_source_sample(source_code, frame_start_sample + i as u64, 0);
                            }
                            active_sources += 1;
                        }
                        code if code >= TX_SRC_INPUT_BASE => {
                            let input_ch = code - TX_SRC_INPUT_BASE;
                            if let Some(block) = input_blocks.get(input_ch) {
                                let peak = block.iter().fold(0.0f32, |m, &v| m.max(v.abs()));
                                if peak > 0.000_001 {
                                    for (dst, src) in frame.iter_mut().zip(block.iter()) { *dst += *src; }
                                    active_sources += 1;
                                }
                            }
                        }
                        _ => {}
                    }
                }

                if active_sources > 1 {
                    let gain = 1.0 / active_sources as f32;
                    for s in frame.iter_mut() { *s *= gain; }
                }
                let peak = frame.iter().fold(0.0f32, |m, &v| m.max(v.abs()));
                meters.set_tx_peak(ch, peak_dbfs_from_peak(peak));
            }

            absolute_sample = absolute_sample.wrapping_add(FRAME_SAMPLES as u64);
            frames_total += 1;
            if last_stats.elapsed() >= STATS_LOG_INTERVAL {
                let elapsed = last_stats.elapsed().as_secs_f64().max(0.001);
                let tx_fps = (frames_total - frames_at_last_stats) as f64 / elapsed;
                frames_at_last_stats = frames_total;
                if let Ok(mut stats) = ui_stats.lock() {
                    stats.tx_fps = tx_fps;
                    stats.tx_peak_dbfs = meters.snapshot_tx(send_channels);
                    stats.input_peak_dbfs = meters.snapshot_input(input_channels);
                }
                last_stats = Instant::now();
            }

            sleep_until(next_deadline);
            next_deadline += frame_dur;
            let now = Instant::now();
            while next_deadline + frame_dur < now { next_deadline += frame_dur; }
        }
    });
}

async fn events_socket(mut socket: WebSocket, state: WebState) {
    loop {
        let mut stats = state.stats.lock().map(|s| s.clone()).unwrap_or_default();
        let rx_n = state.remote_channels.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
        stats.tx_peak_dbfs = state.meters.snapshot_tx(state.local_channels);
        stats.rx_peak_dbfs = state.meters.snapshot_rx(rx_n);
        stats.monitor_peak_dbfs = state.meters.snapshot_monitor();
        let payload = serde_json::json!({
            "status": state.peer_status(),
            "matrix": matrix_for_state(&state),
            "stats": stats,
        });
        if socket.send(Message::Text(payload.to_string().into())).await.is_err() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn spawn_web_ui(addr: String, state: WebState) {
    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => {
                tracing::error!("Web UI runtime failed: {e}");
                return;
            }
        };
        rt.block_on(async move {
            let app = Router::new()
                .route("/", get(index_handler))
                .route("/api/status", get(status_handler))
                .route("/api/audio/devices", get(devices_handler))
                .route("/api/streams", get(streams_handler).post(streams_handler))
                .route("/api/peers", get(peers_handler))
                .route("/api/peers/connect", post(status_handler))
                .route("/api/routes", get(routes_get_handler).post(routes_post_handler))
                .route("/api/local-labels", post(local_labels_post_handler))
                .route("/api/setup/apply", post(setup_apply_handler))
                .route("/api/presets/save", post(preset_save_handler))
                .route("/api/presets/recall", post(preset_recall_handler))
                .route("/api/stats", get(stats_handler))
                .route("/api/remotestatus", get(remotestatus_handler))
                .route("/api/events", get(events_handler))
                .with_state(state);

            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(listener) => listener,
                Err(e) => {
                    tracing::error!("Web UI bind failed on {addr}: {e}");
                    return;
                }
            };
            tracing::info!("M7 Web UI listening on http://{addr}");
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("Web UI server stopped: {e}");
            }
        });
    });
}

// ─── Bidirectional mode (M3/M4) ───────────────────────────────────────────────

pub fn run_bidir(
    remote_host: &str,
    remote_device_name: &str,
    num_channels: usize,
    _source: Source,
    send_enabled: bool,
    recv_enabled: bool,
    device_name_token: [u8; 16],
    shared_token: [u8; 16],
    node_id: String,
    jitter: JitterConfig,
    web_addr: Option<String>,
    monitor_mode: MonitorMode,
    opus_bitrate_per_channel: u32,
    rendezvous_url: Option<String>,
) -> Result<()> {
    if num_channels == 0 || num_channels > MAX_CHANNELS {
        return Err(anyhow!("Channel count must be 1–{MAX_CHANNELS}"));
    }

    let is_initiator = !remote_host.trim().is_empty();
    let remote_addr_str = format!("{remote_host}:{PORT}");

    // Initiator: connect immediately. Responder: bind only and late-connect on first valid probe.
    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{PORT}"))?);
    if is_initiator {
        socket.connect(&remote_addr_str)?;
    }

    // Tracks the established remote address in responder mode.
    // Arc<Mutex<Option<SocketAddr>>> so keepalive and send threads can check before sending.
    let established_addr: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));

    let handshake_connected = Arc::new(AtomicBool::new(false));
    let last_control_ms = Arc::new(AtomicU64::new(0));
    let last_audio_ms = Arc::new(AtomicU64::new(0));
    let remote_channels = Arc::new(AtomicUsize::new(num_channels));
    let remote_metadata = Arc::new(Mutex::new(None));
    let monitor_mode_atomic = Arc::new(AtomicU8::new(monitor_mode.as_u8()));
    let output_route_masks = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    let tx_tone_source_for_send = Arc::new((0..MAX_CHANNELS).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let local_labels = Arc::new(Mutex::new(load_persisted_labels(num_channels)));
    let presets = Arc::new(Mutex::new(HashMap::new()));
    let mut initial_ui_stats = UiStats::default();
    initial_ui_stats.target_ms = jitter.target_delay_ms;
    let ui_stats = Arc::new(Mutex::new(initial_ui_stats));
    let meters = Arc::new(MeterBank::new());
    let devices = Arc::new(scan_audio_devices_once());
    let local_input_channels = if devices.default_input_channels == 0 { 0 } else { devices.default_input_channels.min(MAX_CHANNELS) };
    let initial_routes = load_persisted_routes(local_input_channels, num_channels);
    apply_routes_to_masks(&initial_routes, &output_route_masks);
    apply_routes_to_tx_sources(&initial_routes, &tx_tone_source_for_send);
    let routes = Arc::new(Mutex::new(initial_routes));
    let rtt_us10 = Arc::new(AtomicU32::new(0));
    let remote_conflict: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let web_state = WebState {
        started_at: Instant::now(),
        node_id: node_id.clone(),
        local_channels: num_channels,
        local_input_channels,
        send_enabled,
        recv_enabled,
        handshake_connected: Arc::clone(&handshake_connected),
        last_control_ms: Arc::clone(&last_control_ms),
        last_audio_ms: Arc::clone(&last_audio_ms),
        remote_channels: Arc::clone(&remote_channels),
        remote_metadata: Arc::clone(&remote_metadata),
        monitor_mode: Arc::clone(&monitor_mode_atomic),
        output_route_masks: Arc::clone(&output_route_masks),
        tx_tone_source_for_send: Arc::clone(&tx_tone_source_for_send),
        local_labels: Arc::clone(&local_labels),
        metadata_socket: Arc::clone(&socket),
        device_name_token: device_name_token,
        routes: Arc::clone(&routes),
        presets: Arc::clone(&presets),
        stats: Arc::clone(&ui_stats),
        meters: Arc::clone(&meters),
        runtime: RuntimeSummary {
            mode: if is_initiator { "bidirectional".into() } else { "bidirectional-responder".into() },
            remote_host: if is_initiator { remote_addr_str.clone() } else { String::new() },
            remote_device_name: remote_device_name.to_string(),
            source: "Matrix".into(),
            codec: "Opus".into(),
            opus_bitrate_per_channel,
            frame_ms: 20,
            tx_channels: num_channels,
            token_configured: true,
            token_hint: token_to_hex(&shared_token),
            token_hex: token_to_hex(&shared_token),
            send_enabled,
            recv_enabled,
            latency_ms: jitter.configured_delay_ms,
            effective_latency_ms: jitter.target_delay_ms,
            fixed_jitter: !jitter.adaptive,
            phase_lock: jitter.phase_lock,
            web_note: "Setup Apply performs a controlled process rebuild so the UDP socket, Opus encoders, metadata and routing state are recreated cleanly.".into(),
            link_password_configured: load_persisted_state().config.link_password.map(|p| !p.is_empty()).unwrap_or(false),
            rendezvous_url: rendezvous_url.clone().unwrap_or_default(),
        },
        devices: Arc::clone(&devices),
        restart_lock: Arc::new(Mutex::new(())),
        rtt_us10: Arc::clone(&rtt_us10),
        remote_conflict: Arc::clone(&remote_conflict),
    };
    if let Some(addr) = web_addr {
        spawn_web_ui(addr, web_state);
    }

    tracing::info!(
        "Bidir: role={}  remote_device={remote_device_name}  remote_host={}  channels={num_channels}  \
         send={send_enabled}  recv={recv_enabled}  id={node_id}  \
         receive_buffer={}ms effective={}ms  adaptive_jitter={}  phase_lock={}",
        if is_initiator { "initiator" } else { "responder (waiting for incoming)" },
        if is_initiator { &remote_addr_str } else { "<blank>" },
        jitter.configured_delay_ms, jitter.target_delay_ms, jitter.adaptive, jitter.phase_lock
    );

    // M5 handshake / keepalive thread.
    // Initiator: sends 09 07 probes every 2130ms to punch NAT and keep the connection alive.
    // Responder: only sends keepalive once connected (no address to probe before first handshake).
    // Both modes: sends RTT ping (09 0b) every cycle once connected.
    {
        let socket_hs = Arc::clone(&socket);
        let connected_hs = Arc::clone(&handshake_connected);
        let _local_labels_hs = Arc::clone(&local_labels);
        let rtt_hs = Arc::clone(&rtt_us10);
        let remote_device_name_hs = remote_device_name.to_string(); // owned for 'static move closure
        std::thread::spawn(move || {
            let probe = build_probe_packet(&device_name_token, &shared_token);
            loop {
                let connected = connected_hs.load(Ordering::Relaxed);
                if is_initiator {
                    socket_hs.send(&probe).ok();
                    if connected {
                        tracing::trace!("09 07 keepalive sent");
                        let ts = now_us();
                        socket_hs.send(&build_rtt_ping(ts)).ok();
                        rtt_hs.store(0, Ordering::Relaxed);
                    } else {
                        tracing::trace!("09 07 probe sent");
                    }
                } else if connected {
                    socket_hs.send(&probe).ok();
                    let ts = now_us();
                    socket_hs.send(&build_rtt_ping(ts)).ok();
                    rtt_hs.store(0, Ordering::Relaxed);
                    tracing::trace!("09 07 keepalive sent (responder)");
                } else {
                    tracing::trace!("Responder: waiting for incoming connection from {remote_device_name_hs}");
                }
                std::thread::sleep(std::time::Duration::from_millis(2130));
            }
        });
    }

    // M9 rendezvous: registration keepalive + inbound connect event listener.
    // No-op if rendezvous_url is None — existing direct-IP behaviour is unchanged.
    if let Some(rdv_url) = rendezvous_url.clone().filter(|u| !u.trim().is_empty()) {
        let rdv_base = {
            let u = rdv_url.trim_end_matches('/');
            if u.starts_with("http://") || u.starts_with("https://") {
                u.to_string()
            } else {
                format!("https://{u}")
            }
        };

        // Registration keepalive — POST /api/register every 10 seconds.
        let reg_name = node_id.clone();
        let reg_base = rdv_base.clone();
        std::thread::spawn(move || {
            let body = serde_json::json!({ "name": reg_name, "port": PORT }).to_string();
            loop {
                match ureq::post(&format!("{reg_base}/api/register"))
                    .set("Content-Type", "application/json")
                    .send_string(&body)
                {
                    Ok(_) => tracing::debug!("rendezvous: registered {reg_name}"),
                    Err(e) => tracing::warn!("rendezvous: register failed: {e}"),
                }
                std::thread::sleep(Duration::from_secs(10));
            }
        });

        // Long-poll event listener — GET /api/events/{name}.
        // When a connect event arrives, late-connect the socket and fire a probe
        // to begin NAT hole punching simultaneously with the remote device.
        let event_name = node_id.clone();
        let event_socket = Arc::clone(&socket);
        let event_established = Arc::clone(&established_addr);
        let event_probe_token = device_name_token;
        let event_shared_token = shared_token;
        std::thread::spawn(move || {
            loop {
                let url = format!("{rdv_base}/api/events/{event_name}");
                match ureq::get(&url).call() {
                    Ok(resp) if resp.status() == 200 => {
                        if let Ok(text) = resp.into_string() {
                            if let Ok(evt) = serde_json::from_str::<serde_json::Value>(&text) {
                                let from_name = evt["from_name"].as_str().unwrap_or("unknown");
                                let from_addr = evt["from_addr"].as_str().unwrap_or("");
                                if from_addr.is_empty() { continue; }
                                tracing::info!("rendezvous: inbound connect from {from_name} @ {from_addr}");
                                if let Ok(addr) = from_addr.parse::<std::net::SocketAddr>() {
                                    let already = event_established.lock()
                                        .map(|e| e.is_some()).unwrap_or(false);
                                    if !already {
                                        if event_socket.connect(addr).is_ok() {
                                            if let Ok(mut e) = event_established.lock() {
                                                *e = Some(addr);
                                            }
                                            tracing::info!("rendezvous: late-connected to {addr}");
                                        }
                                    }
                                    let probe = build_probe_packet(&event_probe_token, &event_shared_token);
                                    event_socket.send(&probe).ok();
                                }
                            }
                        }
                    }
                    Ok(_) => {} // 204 No Content = timeout, reconnect immediately
                    Err(e) => {
                        tracing::warn!("rendezvous: events poll failed: {e}");
                        std::thread::sleep(Duration::from_secs(5));
                    }
                }
                std::thread::sleep(Duration::from_millis(200));
            }
        });

        tracing::info!("M9 rendezvous: registered with {rdv_url} as {node_id}");
    }

    let host = cpal::default_host();

    // ── Receive + playback pipeline ───────────────────────────────────────────

    let _out_stream: Option<cpal::Stream> = if recv_enabled {
        let out_device = host.default_output_device().expect("No output device");

        // M5 makes the remote channel count dynamic, so use a stable stereo device
        // stream and map the first N remote channels into it. N=1 copies mono to L/R;
        // N=2 is direct L/R; N>2 is the confirmed stereo summation rule.
        let out_config = cpal::StreamConfig {
            channels: 2,
            sample_rate: cpal::SampleRate(SAMPLE_RATE),
            buffer_size: ALSA_PERIOD,
        };
        tracing::info!(
            "Output: {} @ 48kHz stereo; receive count follows 09 0a metadata",
            out_device.name().unwrap_or_default()
        );

        // Size rings and prime depth from the configured target latency.
        // Ring capacity = 2× target so ASRC has headroom both above and below the setpoint.
        // Minimum ring is PB_RING_SIZE (1s); minimum prime is PRIME_SAMPLES (120ms) so
        // low-latency configs still start quickly.
        let prime_samples = ((jitter.target_delay_ms as usize * SAMPLE_RATE as usize) / 1000)
            .max(PRIME_SAMPLES);
        let ring_samples = (prime_samples * 2).max(PB_RING_SIZE);

        // One lock-free ring per channel.
        // Producers go to the recv thread; consumers go to the output callback.
        let mut pb_prods = Vec::with_capacity(MAX_CHANNELS);
        let mut pb_conss = Vec::with_capacity(MAX_CHANNELS);
        for _ in 0..MAX_CHANNELS {
            let (prod, cons) = HeapRb::<f32>::new(ring_samples).split();
            pb_prods.push(prod);
            pb_conss.push(cons);
        }

        let started = Arc::new(AtomicBool::new(false));
        let started_recv = Arc::clone(&started);
        let started_play = Arc::clone(&started);
        let empty_cb = Arc::new(AtomicU32::new(0));
        let empty_cb_play = Arc::clone(&empty_cb);
        let empty_cb_recv = Arc::clone(&empty_cb);
        let output_underflows = Arc::new(AtomicUsize::new(0));
        let output_underflows_recv = Arc::clone(&output_underflows);
        let output_underflows_play = Arc::clone(&output_underflows);
        let socket_recv = Arc::clone(&socket);
        let connected_recv = Arc::clone(&handshake_connected);
        let last_control_recv = Arc::clone(&last_control_ms);
        let last_audio_recv = Arc::clone(&last_audio_ms);
        let connected_play = Arc::clone(&handshake_connected);
        let remote_channels_recv = Arc::clone(&remote_channels);
        let remote_channels_play = Arc::clone(&remote_channels);
        let remote_metadata_recv = Arc::clone(&remote_metadata);
        let monitor_mode_play = Arc::clone(&monitor_mode_atomic);
        let output_route_masks_play = Arc::clone(&output_route_masks);
        let meters_play = Arc::clone(&meters);
        let ui_stats_recv = Arc::clone(&ui_stats);
        let meters_recv = Arc::clone(&meters);
        let local_labels_recv = Arc::clone(&local_labels);
        let node_id_recv = node_id.clone();
        let rtt_us10_recv = Arc::clone(&rtt_us10);
        let remote_conflict_recv = Arc::clone(&remote_conflict);
        let established_addr_recv = Arc::clone(&established_addr);
        let ring_overflows = Arc::new(AtomicUsize::new(0));
        let ring_overflows_recv = Arc::clone(&ring_overflows);

        // Receive + jitter/decode thread.
        //
        // M6: RTP timestamps are now the receive clock. With phase lock enabled,
        // packets for the same timestamp are held together and emitted as one
        // aligned frame group. Missing/corrupt channels are concealed with Opus
        // PLC after the fixed 10ms phase-lock timeout. With phase lock disabled,
        // packets are decoded to their channel rings immediately, preserving the
        // lower-latency independent-channel behaviour.
        std::thread::spawn(move || {
            unsafe {
                let param = libc::sched_param { sched_priority: 20 };
                let ret = libc::pthread_setschedparam(
                    libc::pthread_self(), libc::SCHED_FIFO, &param
                );
                if ret != 0 {
                    tracing::warn!(
                        "recv thread: SCHED_FIFO priority 20 failed (errno {}). \
                         Running SCHED_OTHER — underflows more likely under VM/load. \
                         Fix: run as root, set rtprio in /etc/security/limits.conf, \
                         or: sudo setcap cap_sys_nice+eip ./audiolinkd",
                        ret
                    );
                } else {
                    tracing::info!("recv thread: SCHED_FIFO priority 20 active");
                }
            }

            socket_recv
                .set_read_timeout(Some(Duration::from_millis(1)))
                .ok();

            let mut decoders: Vec<opus::Decoder> = (0..MAX_CHANNELS)
                .map(|_| opus::Decoder::new(SAMPLE_RATE, opus::Channels::Mono).unwrap())
                .collect();

            // One Rubato FastFixedIn resampler per channel.
            // Ratio 1.0 at init; updated each loop iteration from correction_ratio.
            // PolynomialDegree::Linear gives transparent interpolation at sub-500ppm
            // rates with negligible CPU cost. The max_relative of 1.01 covers our
            // ±5000ppm control clamp with headroom.
            let mut resamplers: Vec<FastFixedIn<f32>> = (0..MAX_CHANNELS)
                .map(|_| FastFixedIn::new(
                    1.0,
                    1.01,
                    PolynomialDegree::Linear,
                    FRAME_SAMPLES,
                    1,
                ).expect("Rubato resampler init failed"))
                .collect();

            let mut buf = vec![0u8; 65535];
            let mut pcm = vec![0f32; FRAME_SAMPLES];
            let mut decoded_frame: Vec<Vec<f32>> = vec![vec![0.0; FRAME_SAMPLES]; MAX_CHANNELS];
            let mut last_remote_metadata: Option<RemoteMetadata> = None;
            let mut groups: BTreeMap<u32, FrameGroup> = BTreeMap::new();
            let mut newest_timestamp: Option<u32> = None;
            let mut last_seq: Vec<Option<u16>> = vec![None; MAX_CHANNELS];
            let mut last_stats = Instant::now();
            let mut plc_count_total: usize = 0;
            let mut lost_packets_total: usize = 0;
            let mut decoded_groups_total: usize = 0;
            let mut decoded_groups_at_last_stats: usize = 0;
            let mut media_packets_total: usize = 0;
            let mut media_packets_at_last_stats: usize = 0;
            let mut rx_bytes_total: usize = 0;
            let mut rx_bytes_at_last_stats: usize = 0;
            let mut last_jitter_timestamp: Option<u32> = None;
            let mut last_jitter_arrival: Option<Instant> = None;
            let mut jitter_ms_estimate: f64 = 0.0;
            let mut output_underflows_at_last_stats: usize = 0;
            let mut plc_count_at_last_stats: usize = 0;
            let mut lost_packets_at_last_stats: usize = 0;
            let mut correction_ratio: f64 = 1.0; // updated each iteration; 1.0 on first drain is correct
            if jitter.phase_lock {
                tracing::info!("M6 jitter: phase-locked timestamp buffer active; Rubato ASRC active");
            }

            loop {
                // In responder mode we use recv_from so we can extract the sender's address
                // for the late-connect. In initiator mode we use recv (socket is already connected).
                let recv_result = if is_initiator {
                    socket_recv.recv(&mut buf).map(|n| (n, None))
                } else {
                    socket_recv.recv_from(&mut buf).map(|(n, addr)| (n, Some(addr)))
                };

                match recv_result {
                    Ok((n, src_addr_opt)) => {
                        // Responder: late-connect on first valid handshake or media packet.
                        if !is_initiator {
                            if let Some(src_addr) = src_addr_opt {
                                let mut established = established_addr_recv.lock().unwrap();
                                match *established {
                                    None => {
                                        // First packet from any source — connect to it.
                                        if let Err(e) = socket_recv.connect(src_addr) {
                                            tracing::error!("Responder: late-connect to {src_addr} failed: {e}");
                                        } else {
                                            tracing::info!("Responder: late-connected to {src_addr}");
                                            *established = Some(src_addr);
                                        }
                                    }
                                    Some(known) if known != src_addr => {
                                        // Packet from a different address while already connected — conflict.
                                        tracing::warn!(
                                            "Connection conflict: packet from {src_addr} but already connected to {known}"
                                        );
                                        if let Ok(mut c) = remote_conflict_recv.lock() {
                                            if c.is_none() {
                                                *c = Some(src_addr.to_string());
                                            }
                                        }
                                        continue; // reject this packet
                                    }
                                    _ => {}
                                }
                            }
                        }

                        if let Some(pkt) = parse_handshake_packet(&buf[..n]) {
                            last_control_recv.store(now_millis(), Ordering::Relaxed);
                            match pkt {
                                HandshakePacket::Probe { sender_token, expected_peer } if expected_peer == shared_token => {
                                    // If we now know the sender's device name from metadata, use it for conflict reporting.
                                    // For now store sender_token hex as the conflict identifier.
                                    let sender_name = last_remote_metadata.as_ref()
                                        .and_then(|m| m.node_id.clone())
                                        .unwrap_or_else(|| token_to_hex(&sender_token));
                                    socket_recv.send(&build_accept_packet(&shared_token)).ok();
                                    socket_recv.send(&build_confirm_packet(&shared_token)).ok();
                                    socket_recv
                                        .send(&build_metadata_packet_with_labels(
                                        &device_name_token,
                                        num_channels,
                                        &node_id_recv,
                                        &local_labels_recv.lock().map(|l| l.clone()).unwrap_or_else(|_| (0..num_channels).map(local_channel_label).collect()),
                                    ))
                                        .ok();
                                    if !connected_recv.swap(true, Ordering::Relaxed) {
                                        tracing::info!(
                                            "Handshake: received 09 07 from {sender_name}, sent 09 09 / 09 08 / 09 0a"
                                        );
                                        // Clear any stale conflict once a fresh valid handshake completes.
                                        if let Ok(mut c) = remote_conflict_recv.lock() { *c = None; }
                                    }
                                }
                                HandshakePacket::Accept { token } if token == shared_token => {
                                    socket_recv.send(&build_confirm_packet(&shared_token)).ok();
                                    socket_recv
                                        .send(&build_metadata_packet_with_labels(
                                        &device_name_token,
                                        num_channels,
                                        &node_id_recv,
                                        &local_labels_recv.lock().map(|l| l.clone()).unwrap_or_else(|_| (0..num_channels).map(local_channel_label).collect()),
                                    ))
                                        .ok();
                                    if !connected_recv.swap(true, Ordering::Relaxed) {
                                        tracing::info!("Handshake: received 09 09, sent 09 08 / 09 0a");
                                        if let Ok(mut c) = remote_conflict_recv.lock() { *c = None; }
                                    }
                                }
                                HandshakePacket::Confirm { token } if token == shared_token => {
                                    if !connected_recv.swap(true, Ordering::Relaxed) {
                                        tracing::info!("Handshake: received 09 08 confirmation");
                                        if let Ok(mut c) = remote_conflict_recv.lock() { *c = None; }
                                    }
                                }
                                HandshakePacket::Metadata(metadata) if metadata.channels > 0 => {
                                    let mut metadata = metadata;
                                    metadata.channels = metadata.channels.min(MAX_CHANNELS);

                                    if last_remote_metadata.as_ref() == Some(&metadata) {
                                        tracing::trace!(
                                            "Handshake: duplicate 09 0a metadata from {} ignored",
                                            metadata.display_name()
                                        );
                                        continue;
                                    }

                                    let old_channels = last_remote_metadata
                                        .as_ref()
                                        .map(|m| m.channels)
                                        .unwrap_or_else(|| remote_channels_recv.load(Ordering::Relaxed));
                                    let channels_changed = metadata.channels != old_channels;

                                    remote_channels_recv.store(metadata.channels, Ordering::Relaxed);
                                    if channels_changed {
                                        groups.clear();
                                        started_recv.store(false, Ordering::Relaxed);
                                    }

                                    tracing::info!(
                                        "Handshake: received 09 0a metadata from {} — remote has {} channels: {:?}",
                                        metadata.display_name(),
                                        metadata.channels,
                                        metadata.labels
                                    );
                                    if let Ok(mut shared_meta) = remote_metadata_recv.lock() {
                                        *shared_meta = Some(metadata.clone());
                                    }
                                    last_remote_metadata = Some(metadata);
                                }
                                HandshakePacket::RttPing { timestamp_us } => {
                                    // Echo back immediately — do not alter the timestamp.
                                    socket_recv.send(&build_rtt_pong(timestamp_us)).ok();
                                }
                                HandshakePacket::RttPong { timestamp_us } => {
                                    let now = now_us();
                                    if now >= timestamp_us {
                                        let rtt_us = now - timestamp_us;
                                        // Store as us×10 for 0.1ms resolution in a u32 atomic.
                                        rtt_us10_recv.store((rtt_us * 10 / 1000) as u32, Ordering::Relaxed);
                                        tracing::debug!("RTT: {:.1}ms", rtt_us as f64 / 1000.0);
                                    }
                                }
                                _ => {}
                            }
                            continue;
                        }

                        let pkt = match parse_packet(&buf[..n]) {
                            Some(p) => {
                                last_audio_recv.store(now_millis(), Ordering::Relaxed);
                                p
                            },
                            None => continue,
                        };

                        media_packets_total = media_packets_total.saturating_add(1);
                        rx_bytes_total = rx_bytes_total.saturating_add(n);
                        let arrival = Instant::now();
                        if Some(pkt.timestamp) != last_jitter_timestamp {
                            if let (Some(prev_ts), Some(prev_arrival)) = (last_jitter_timestamp, last_jitter_arrival) {
                                let expected_ms = timestamp_elapsed_samples(pkt.timestamp, prev_ts) as f64 * 1000.0 / SAMPLE_RATE as f64;
                                if expected_ms > 0.0 && expected_ms < 1000.0 {
                                    let arrival_ms = arrival.duration_since(prev_arrival).as_secs_f64() * 1000.0;
                                    let delta = (arrival_ms - expected_ms).abs();
                                    jitter_ms_estimate += (delta - jitter_ms_estimate) / 16.0;
                                }
                            }
                            last_jitter_timestamp = Some(pkt.timestamp);
                            last_jitter_arrival = Some(arrival);
                        }

                        let ch = pkt.channel as usize;
                        if ch >= MAX_CHANNELS {
                            tracing::warn!(
                                "Received ch{ch} exceeds MAX_CHANNELS ({MAX_CHANNELS}) — ignoring"
                            );
                            continue;
                        }

                        if let Some(prev) = last_seq[ch] {
                            let expected = prev.wrapping_add(1);
                            if pkt.seq != expected {
                                let gap = pkt.seq.wrapping_sub(expected) as usize;
                                if gap > 0 && gap < 10_000 {
                                    lost_packets_total += gap;
                                    tracing::debug!(
                                        "ch{ch}: RTP sequence gap, expected {expected}, got {} ({gap} missing)",
                                        pkt.seq
                                    );
                                }
                            }
                        }
                        last_seq[ch] = Some(pkt.seq);

                        let active = remote_channels_recv.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
                        newest_timestamp = Some(match newest_timestamp {
                            Some(prev) if timestamp_elapsed_samples(pkt.timestamp, prev) < 0x8000_0000 => {
                                pkt.timestamp
                            }
                            Some(prev) => prev,
                            None => pkt.timestamp,
                        });

                        if jitter.phase_lock {
                            let group = groups
                                .entry(pkt.timestamp)
                                .or_insert_with(|| FrameGroup::new(pkt.timestamp, active));
                            group.insert(ch, pkt.opus_payload);
                        } else {
                            let was_real = decode_or_plc(
                                &mut decoders[ch],
                                Some(pkt.opus_payload),
                                &mut pcm,
                            );
                            if !was_real {
                                plc_count_total += 1;
                            }
                            // Non-phase-lock: decode immediately and resample per channel.
                            let mut peak = 0.0f32;
                            resamplers[ch].set_resample_ratio(correction_ratio, false).ok();
                            match resamplers[ch].process(&[pcm.as_slice()], None) {
                                Ok(out_waves) => {
                                    for &s in out_waves[0].iter() {
                                        peak = peak.max(s.abs());
                                        if pb_prods[ch].try_push(s).is_err() {
                                            ring_overflows_recv.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                                Err(_) => {
                                    for &s in pcm.iter() {
                                        peak = peak.max(s.abs());
                                        if pb_prods[ch].try_push(s).is_err() {
                                            ring_overflows_recv.fetch_add(1, Ordering::Relaxed);
                                        }
                                    }
                                }
                            }
                            meters_recv.set_rx_peak(ch, peak_dbfs_from_peak(peak));
                        }

                        empty_cb_recv.store(0, Ordering::Relaxed);
                    }
                    Err(e)
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                        // ICMP port-unreachable from remote — their socket is closed.
                        // Unlike WouldBlock, ECONNREFUSED bypasses the recv timeout and
                        // returns immediately, so we must sleep here or we spin at 100%
                        // CPU and starve the audio callback. Mark disconnected so the
                        // keepalive thread resumes probing, then sleep 100ms per retry.
                        if connected_recv.swap(false, Ordering::Relaxed) {
                            tracing::info!("recv: remote disconnected (ECONNREFUSED) — waiting for reconnect");
                        }
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    Err(e) => {
                        tracing::error!("recv: {e}");
                        continue;
                    }
                }

                let active = remote_channels_recv.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);

                if jitter.phase_lock {
                    let (out, plc) = drain_phase_locked_groups(
                        &mut groups,
                        &mut decoders,
                        active,
                        &mut decoded_frame,
                        &mut pcm,
                        &mut |decoded, active_ch| {
                            for ch in 0..active_ch {
                                resamplers[ch].set_resample_ratio(correction_ratio, false).ok();
                                match resamplers[ch].process(&[decoded[ch].as_slice()], None) {
                                    Ok(out_waves) => {
                                        let mut peak = 0.0f32;
                                        for &s in out_waves[0].iter() {
                                            peak = peak.max(s.abs());
                                            if pb_prods[ch].try_push(s).is_err() {
                                                ring_overflows_recv.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                        meters_recv.set_rx_peak(ch, peak_dbfs_from_peak(peak));
                                    }
                                    Err(_) => {
                                        // Fallback: direct write if resampler fails (shouldn't happen)
                                        for &s in decoded[ch].iter() {
                                            if pb_prods[ch].try_push(s).is_err() {
                                                ring_overflows_recv.fetch_add(1, Ordering::Relaxed);
                                            }
                                        }
                                    }
                                }
                            }
                        },
                    );
                    decoded_groups_total += out;
                    plc_count_total += plc;
                }

                let fill_ms = ring_fill_ms(&pb_prods, active);
                let target_fill_ms = jitter.target_delay_ms;

                // M6 baseline: phase-lock and PLC are active, but clock-drift correction
                // is not yet applied to audio. Report drift pressure only. Target latency
                // stays fixed; timestamp groups are only held for packet alignment/timeout.
                let fill_error_ms = fill_ms as f64 - target_fill_ms as f64;
                correction_ratio = if jitter.adaptive {
                    (1.0 - fill_error_ms * 0.0000025).clamp(0.995, 1.005)
                } else {
                    1.0
                };

                if !started_recv.load(Ordering::Relaxed)
                    && pb_prods[..active]
                        .iter()
                        .all(|p| p.occupied_len() >= prime_samples)
                {
                    started_recv.store(true, Ordering::Relaxed);
                    tracing::info!(
                        "All {active} receive channels primed ({}ms) — starting playback",
                        prime_samples * 1000 / SAMPLE_RATE as usize
                    );
                }

                if last_stats.elapsed() >= STATS_LOG_INTERVAL {
                    let ppm = ((correction_ratio - 1.0) * 1_000_000.0).round() as isize;
                    let elapsed = last_stats.elapsed().as_secs_f64().max(0.001);
                    let rx_fps = (decoded_groups_total - decoded_groups_at_last_stats) as f64 / elapsed;
                    decoded_groups_at_last_stats = decoded_groups_total;
                    let media_packets_delta = media_packets_total.saturating_sub(media_packets_at_last_stats);
                    media_packets_at_last_stats = media_packets_total;
                    let rx_bytes_delta = rx_bytes_total.saturating_sub(rx_bytes_at_last_stats);
                    rx_bytes_at_last_stats = rx_bytes_total;
                    let rx_mbps = rx_bytes_delta as f64 * 8.0 / elapsed / 1_000_000.0;
                    let output_underflows_now = output_underflows_recv.load(Ordering::Relaxed);
                    let output_underflows_delta = output_underflows_now.saturating_sub(output_underflows_at_last_stats);
                    output_underflows_at_last_stats = output_underflows_now;
                    let plc_delta = plc_count_total.saturating_sub(plc_count_at_last_stats);
                    plc_count_at_last_stats = plc_count_total;
                    let seq_missing_delta = lost_packets_total.saturating_sub(lost_packets_at_last_stats);
                    lost_packets_at_last_stats = lost_packets_total;
                    let loss_den = media_packets_delta.saturating_add(seq_missing_delta);
                    let loss_percent = if loss_den > 0 { seq_missing_delta as f64 * 100.0 / loss_den as f64 } else { 0.0 };
                    let rtt_raw = rtt_us10_recv.load(Ordering::Relaxed);
                    let rtt_ms = rtt_raw as f64 / 10.0;
                    let one_way_ms = rtt_ms / 2.0;
                    let overflows = ring_overflows_recv.load(Ordering::Relaxed);
                    tracing::info!(
                        "RX stats: channels={active} rx={:.3}Mbps loss={:.3}% jitter={:.2}ms fill={}ms target={}ms \
                         phase_lock={} queued_groups={} decoded_fps={:.1} output_underflows={} plc_channels={} \
                         seq_missing={} drift_pressure={}ppm rtt={:.1}ms one_way={:.1}ms ring_overflows={}",
                        rx_mbps, loss_percent, jitter_ms_estimate, fill_ms, target_fill_ms,
                        jitter.phase_lock, groups.len(), rx_fps, output_underflows_delta,
                        plc_delta, seq_missing_delta, ppm, rtt_ms, one_way_ms, overflows
                    );
                    if let Ok(mut stats) = ui_stats_recv.lock() {
                        stats.channels = active;
                        stats.fill_ms = fill_ms;
                        stats.target_ms = target_fill_ms;
                        stats.phase_lock = jitter.phase_lock;
                        stats.queued_groups = groups.len();
                        stats.decoded_fps = rx_fps;
                        stats.output_underflows = output_underflows_delta;
                        stats.plc_channels = plc_delta;
                        stats.seq_missing = seq_missing_delta;
                        stats.loss_percent = loss_percent;
                        stats.jitter_ms = jitter_ms_estimate;
                        stats.latency_ms = fill_ms;
                        stats.rx_mbps = rx_mbps;
                        stats.drift_pressure_ppm = ppm;
                        stats.rtt_ms = rtt_ms;
                        stats.one_way_latency_ms = one_way_ms;
                        stats.ring_overflows = overflows;
                        stats.rx_peak_dbfs = meters_recv.snapshot_rx(active);
                        stats.monitor_peak_dbfs = meters_recv.snapshot_monitor();
                    }
                    last_stats = Instant::now();
                }
            }
        });

        // Output callback.
        // ASRC is now handled in the recv/decode thread via Rubato before writing to rings.
        // This callback just pops from the rings and mixes to stereo — no sample
        // manipulation here keeps the hot path clean and avoids phase discontinuities.
        let stream = out_device.build_output_stream(
            &out_config,
            move |data: &mut [f32], _| {
                if !connected_play.load(Ordering::Relaxed) || !started_play.load(Ordering::Relaxed) {
                    data.fill(0.0);
                    return;
                }

                let active_channels = remote_channels_play.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
                let active_for_gain = meters_play.snapshot_rx(active_channels);
                let mode = MonitorMode::from_u8(monitor_mode_play.load(Ordering::Relaxed));
                let left_mask = output_route_masks_play[0].load(Ordering::Relaxed);
                let right_mask = output_route_masks_play[1].load(Ordering::Relaxed);
                let mut any_sample = false;
                let mut underflowed = false;

                let mut rx_peaks = [0.0f32; MAX_CHANNELS];
                let mut monitor_peaks = [0.0f32; 2];

                for frame in data.chunks_exact_mut(2) {
                    let mut samples = [0.0f32; MAX_CHANNELS];

                    for ch in 0..active_channels {
                        match pb_conss[ch].try_pop() {
                            Some(v) => {
                                any_sample = true;
                                samples[ch] = v;
                                rx_peaks[ch] = rx_peaks[ch].max(v.abs());
                            }
                            None => {
                                underflowed = true;
                            }
                        }
                    }

                    match mode {
                        MonitorMode::CompatStereoAlternate => {
                            let mut l = 0.0f32;
                            let mut r = 0.0f32;
                            if active_channels == 1 {
                                l = samples[0];
                                r = samples[0];
                            } else {
                                for ch in 0..active_channels {
                                    if ch % 2 == 0 { l += samples[ch]; } else { r += samples[ch]; }
                                }
                            }
                            frame[0] = l;
                            frame[1] = r;
                        }
                        MonitorMode::PatchMatrix => {
                            let mut l = 0.0f32;
                            let mut r = 0.0f32;
                            let mut l_count = 0u32;
                            let mut r_count = 0u32;
                            for ch in 0..active_channels {
                                let bit = 1u64 << ch;
                                let carries_signal = active_for_gain.get(ch).copied().unwrap_or(-120.0) > -80.0
                                    || samples[ch].abs() > 0.000_01;
                                if left_mask & bit != 0 {
                                    l += samples[ch];
                                    if carries_signal { l_count += 1; }
                                }
                                if right_mask & bit != 0 {
                                    r += samples[ch];
                                    if carries_signal { r_count += 1; }
                                }
                            }
                            frame[0] = if l_count > 0 { l / l_count as f32 } else { l };
                            frame[1] = if r_count > 0 { r / r_count as f32 } else { r };
                        }
                    }
                }

                for ch in 0..active_channels { meters_play.set_rx_peak(ch, peak_dbfs_from_peak(rx_peaks[ch])); }
                for frame in data.chunks_exact(2) {
                    monitor_peaks[0] = monitor_peaks[0].max(frame[0].abs());
                    monitor_peaks[1] = monitor_peaks[1].max(frame[1].abs());
                }
                meters_play.set_monitor_peak(0, peak_dbfs_from_peak(monitor_peaks[0]));
                meters_play.set_monitor_peak(1, peak_dbfs_from_peak(monitor_peaks[1]));

                if data.len() % 2 != 0 {
                    if let Some(last) = data.last_mut() {
                        *last = 0.0;
                    }
                }

                if underflowed {
                    output_underflows_play.fetch_add(1, Ordering::Relaxed);
                }

                handle_underrun(!any_sample, &started_play, &empty_cb_play);
            },
            |e| tracing::error!("Output error: {e}"),
            None,
        )?;

        stream.play()?;
        Some(stream)
    } else {
        tracing::info!("Receive disabled (--no-recv)");
        None
    };

    // ── Send pipeline ─────────────────────────────────────────────────────────

    let _in_stream: Option<cpal::Stream> = if send_enabled {
        let socket_send = Arc::clone(&socket);
        let connected_send = Arc::clone(&handshake_connected);
        let ui_stats_send = Arc::clone(&ui_stats);
        let meters_send = Arc::clone(&meters);
        let tx_source_masks_send = Arc::clone(&tx_tone_source_for_send);

        tracing::info!(
            "Input/test sources: active Web UI send routing matrix across {} channel(s)",
            num_channels
        );

        // Capture the default physical input when available. Local I/O is
        // independent from the network stream width: the matrix exposes the
        // device's capture channels as Local Input 1..N and only sends them
        // when explicitly patched to Network Send channels.
        let input_channels = local_input_channels.min(MAX_CHANNELS);
        let input_rings: Arc<Mutex<Vec<VecDeque<f32>>>> = Arc::new(Mutex::new(
            (0..input_channels).map(|_| VecDeque::with_capacity(CAP_RING_SIZE)).collect(),
        ));
        let input_stream = if input_channels > 0 {
            match host.default_input_device() {
                Some(in_device) => {
                    let in_config = cpal::StreamConfig {
                        channels: input_channels as u16,
                        sample_rate: cpal::SampleRate(SAMPLE_RATE),
                        buffer_size: ALSA_PERIOD,
                    };
                    tracing::info!(
                        "Input: {} @ 48kHz {} channel(s) → send routing matrix",
                        in_device.name()?,
                        input_channels
                    );
                    let rings_cb = Arc::clone(&input_rings);
                    let meters_cb = Arc::clone(&meters);
                    let mut peak_acc = vec![0.0f32; input_channels];
                    let mut sample_count: usize = 0;
                    let stream = in_device.build_input_stream(
                        &in_config,
                        move |data: &[f32], _| {
                            if let Ok(mut rings) = rings_cb.lock() {
                                for frame in data.chunks(input_channels) {
                                    for ch in 0..input_channels {
                                        let s = frame.get(ch).copied().unwrap_or(0.0);
                                        if let Some(ring) = rings.get_mut(ch) {
                                            if ring.len() >= CAP_RING_SIZE {
                                                ring.pop_front();
                                            }
                                            ring.push_back(s);
                                        }
                                        peak_acc[ch] = peak_acc[ch].max(s.abs());
                                    }
                                    sample_count += 1;
                                    if sample_count >= FRAME_SAMPLES {
                                        for ch in 0..input_channels {
                                            meters_cb.set_input_peak(ch, peak_dbfs_from_peak(peak_acc[ch]));
                                            peak_acc[ch] = 0.0;
                                        }
                                        sample_count = 0;
                                    }
                                }
                            }
                        },
                        |e| tracing::error!("Input error: {e}"),
                        None,
                    )?;
                    stream.play()?;
                    Some(stream)
                }
                None => {
                    tracing::warn!("No default input device; Local Input sources will remain silent");
                    None
                }
            }
        } else {
            tracing::info!("No capture channels available on the default input device");
            None
        };

        let input_rings_send = Arc::clone(&input_rings);
        std::thread::spawn(move || {
            let mut encoders: Vec<opus::Encoder> =
                (0..num_channels).map(|_| make_encoder_with_bitrate(opus_bitrate_per_channel).unwrap()).collect();
            let mut seqs = vec![0u16; num_channels];
            let mut ts: u32 = 0;
            let mut frame_count: u64 = 0;
            let mut absolute_sample: u64 = 0;

            let mut frames = vec![vec![0f32; FRAME_SAMPLES]; num_channels];
            let mut compressed = vec![0u8; 4000];
            let frame_dur = Duration::from_micros(20_000);
            let mut next_deadline = Instant::now() + frame_dur;
            let mut sent_frames_total: u64 = 0;
            let mut sent_frames_at_last_stats: u64 = 0;
            let mut tx_bytes_total: usize = 0;
            let mut tx_bytes_at_last_stats: usize = 0;
            let mut last_tx_stats = Instant::now();

            loop {
                let connected_now = connected_send.load(Ordering::Relaxed);
                let frame_start_sample = absolute_sample;

                // Pull one 20 ms block from each local input channel. If the
                // device underruns or is not present, silence is used for that
                // part of the matrix only; EBU source generation continues.
                let mut input_blocks = vec![vec![0.0f32; FRAME_SAMPLES]; input_channels];
                if input_channels > 0 {
                    if let Ok(mut rings) = input_rings_send.lock() {
                        for ch in 0..input_channels {
                            if let Some(ring) = rings.get_mut(ch) {
                                for i in 0..FRAME_SAMPLES {
                                    input_blocks[ch][i] = ring.pop_front().unwrap_or(0.0);
                                }
                            }
                        }
                    }
                }

                for ch in 0..num_channels {
                    let mask = tx_source_masks_send
                        .get(ch)
                        .map(|v| v.load(Ordering::Relaxed) as u64)
                        .unwrap_or(0);

                    let frame = &mut frames[ch];
                    frame.fill(0.0);

                    let mut active_sources = 0usize;
                    for bit in 0..64 {
                        if (mask & (1u64 << bit)) == 0 { continue; }
                        let Some(source_code) = source_code_from_bit_index(bit) else { continue; };
                        match source_code {
                            TX_SRC_EBU_L | TX_SRC_EBU_R => {
                                for (i, s) in frame.iter_mut().enumerate() {
                                    *s += tx_source_sample(source_code, frame_start_sample + i as u64, 0);
                                }
                                active_sources += 1;
                            }
                            code if code >= TX_SRC_INPUT_BASE => {
                                let input_ch = code - TX_SRC_INPUT_BASE;
                                if let Some(block) = input_blocks.get(input_ch) {
                                    let peak = block.iter().fold(0.0f32, |m, &v| m.max(v.abs()));
                                    if peak > 0.000_001 {
                                        for (dst, src) in frame.iter_mut().zip(block.iter()) {
                                            *dst += *src;
                                        }
                                        active_sources += 1;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }

                    if active_sources > 1 {
                        let gain = 1.0 / active_sources as f32;
                        for s in frame.iter_mut() { *s *= gain; }
                    }

                    let peak = frame.iter().fold(0.0f32, |m, &v| m.max(v.abs()));
                    meters_send.set_tx_peak(ch, peak_dbfs_from_peak(peak));

                    if connected_now {
                        if let Ok(n) = encoders[ch].encode_float(frame, &mut compressed) {
                            let pkt = build_packet(seqs[ch], ts, ch as u8, &compressed[..n]);
                            tx_bytes_total = tx_bytes_total.saturating_add(pkt.len());
                            socket_send.send(&pkt).ok();
                        }
                        seqs[ch] = seqs[ch].wrapping_add(1);
                    }
                }

                absolute_sample = absolute_sample.wrapping_add(FRAME_SAMPLES as u64);
                ts = ts.wrapping_add(FRAME_SAMPLES as u32);
                frame_count += 1;

                sent_frames_total += 1;
                if last_tx_stats.elapsed() >= STATS_LOG_INTERVAL {
                    let elapsed = last_tx_stats.elapsed().as_secs_f64().max(0.001);
                    let tx_fps = (sent_frames_total - sent_frames_at_last_stats) as f64 / elapsed;
                    sent_frames_at_last_stats = sent_frames_total;
                    let tx_bytes_delta = tx_bytes_total.saturating_sub(tx_bytes_at_last_stats);
                    tx_bytes_at_last_stats = tx_bytes_total;
                    let tx_mbps = tx_bytes_delta as f64 * 8.0 / elapsed / 1_000_000.0;
                    tracing::info!(
                        "TX stats: source=matrix tx={:.3}Mbps frames_per_sec={:.1} frame={} timestamp={}",
                        tx_mbps,
                        tx_fps,
                        frame_count,
                        ts
                    );
                    if let Ok(mut stats) = ui_stats_send.lock() {
                        stats.tx_fps = tx_fps;
                        stats.tx_mbps = tx_mbps;
                        stats.tx_active_channel = 0;
                        stats.tx_peak_dbfs = meters_send.snapshot_tx(num_channels);
                        stats.input_peak_dbfs = meters_send.snapshot_input(input_channels);
                    }
                    last_tx_stats = Instant::now();
                }

                sleep_until(next_deadline);
                next_deadline += frame_dur;

                let now = Instant::now();
                while next_deadline + frame_dur < now {
                    next_deadline += frame_dur;
                }
            }
        });

        input_stream
    } else {
        tracing::info!("Send disabled (--no-send)");
        None
    };

    tracing::info!("Running — Ctrl+C to stop");

    loop {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
}

// ─── Legacy single-channel modes (M2) ────────────────────────────────────────

fn run_sender(dest_ip: &str) -> Result<()> {
    let dest = format!("{dest_ip}:{PORT}");
    let socket = UdpSocket::bind("0.0.0.0:0")?;
    socket.connect(&dest)?;

    let host = cpal::default_host();
    let device = host.default_input_device().expect("No input device");
    let config = cpal::StreamConfig {
        channels: 1,
        sample_rate: cpal::SampleRate(SAMPLE_RATE),
        buffer_size: ALSA_PERIOD,
    };
    tracing::info!("Sending to {dest}");
    tracing::info!("Input: {} @ 48kHz mono", device.name()?);

    let (mut cap_prod, mut cap_cons) = HeapRb::<f32>::new(CAP_RING_SIZE).split();
    let stream = device.build_input_stream(
        &config,
        move |data: &[f32], _| { for &s in data { cap_prod.try_push(s).ok(); } },
        |e| tracing::error!("Input error: {e}"),
        None,
    )?;
    stream.play()?;
    tracing::info!("Sending — Ctrl+C to stop");

    let mut encoder = make_encoder()?;
    let mut frame = vec![0f32; FRAME_SAMPLES];
    let mut compressed = vec![0u8; 4000];
    let mut seq: u16 = 0;
    let mut ts: u32 = 0;

    loop {
        if cap_cons.occupied_len() < FRAME_SAMPLES {
            std::thread::sleep(std::time::Duration::from_micros(200));
            continue;
        }
        for s in frame.iter_mut() { *s = cap_cons.try_pop().unwrap_or(0.0); }
        if let Ok(n) = encoder.encode_float(&frame, &mut compressed) {
            socket.send(&build_packet(seq, ts, 0, &compressed[..n])).ok();
        }
        seq = seq.wrapping_add(1);
        ts = ts.wrapping_add(FRAME_SAMPLES as u32);
    }
}

fn run_receiver() -> Result<()> {
    let socket = UdpSocket::bind(format!("0.0.0.0:{PORT}"))?;
    tracing::info!("Listening on 0.0.0.0:{PORT}");

    let host = cpal::default_host();
    let device = host.default_output_device().expect("No output device");
    let config = cpal::StreamConfig {
        channels: 2,
        sample_rate: cpal::SampleRate(SAMPLE_RATE),
        buffer_size: ALSA_PERIOD,
    };
    tracing::info!("Output: {} @ 48kHz stereo", device.name()?);

    let (mut pb_prod, mut pb_cons) = HeapRb::<f32>::new(PB_RING_SIZE).split();
    let started = Arc::new(AtomicBool::new(false));
    let started_recv = Arc::clone(&started);
    let started_play = Arc::clone(&started);
    let empty_cb = Arc::new(AtomicU32::new(0));
    let empty_cb_play = Arc::clone(&empty_cb);
    let empty_cb_recv = Arc::clone(&empty_cb);

    std::thread::spawn(move || {
        unsafe {
            let param = libc::sched_param { sched_priority: 20 };
            libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_FIFO, &param);
        }
        let mut decoder = opus::Decoder::new(SAMPLE_RATE, opus::Channels::Mono).unwrap();
        let mut buf = vec![0u8; 65535];
        let mut pcm = vec![0f32; FRAME_SAMPLES];

        loop {
            let n = match socket.recv(&mut buf) {
                Ok(n) => n,
                Err(e) => { tracing::error!("recv: {e}"); continue; }
            };
            let pkt = match parse_packet(&buf[..n]) {
                Some(p) => p,
                None => continue,
            };
            if decoder.decode_float(pkt.opus_payload, &mut pcm, false).is_err() { continue; }
            for &s in &pcm { pb_prod.try_push(s).ok(); }
            empty_cb_recv.store(0, Ordering::Relaxed);
            if !started_recv.load(Ordering::Relaxed) && pb_prod.occupied_len() >= PRIME_SAMPLES {
                started_recv.store(true, Ordering::Relaxed);
                tracing::info!("Buffer primed — starting playback");
            }
        }
    });

    let stream = device.build_output_stream(
        &config,
        move |data: &mut [f32], _| {
            if !started_play.load(Ordering::Relaxed) { data.fill(0.0); return; }
            let mut last = 0.0f32;
            let mut all_empty = true;
            for chunk in data.chunks_mut(2) {
                let s = match pb_cons.try_pop() {
                    Some(s) => { last = s; all_empty = false; s }
                    None => { last *= UNDERRUN_DECAY; last }
                };
                chunk[0] = s;
                chunk[1] = s;
            }
            handle_underrun(all_empty, &started_play, &empty_cb_play);
        },
        |e| tracing::error!("Output error: {e}"),
        None,
    )?;
    stream.play()?;
    tracing::info!("Receiving — Ctrl+C to stop");
    loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
}

fn run_echo_test(server_ip: &str) -> Result<()> {
    let dest = format!("{server_ip}:{PORT}");
    let socket = Arc::new(UdpSocket::bind(format!("0.0.0.0:{PORT}"))?);
    socket.connect(&dest)?;
    tracing::info!("Echo test via {dest}");

    let host = cpal::default_host();
    let in_device = host.default_input_device().expect("No input");
    let out_device = host.default_output_device().expect("No output");
    tracing::info!("Input: {}  Output: {}", in_device.name()?, out_device.name()?);

    let in_config = cpal::StreamConfig {
        channels: 1,
        sample_rate: cpal::SampleRate(SAMPLE_RATE),
        buffer_size: ALSA_PERIOD,
    };
    let out_config = cpal::StreamConfig {
        channels: 2,
        sample_rate: cpal::SampleRate(SAMPLE_RATE),
        buffer_size: ALSA_PERIOD,
    };

    let (mut pb_prod, mut pb_cons) = HeapRb::<f32>::new(PB_RING_SIZE).split();
    let (mut cap_prod, mut cap_cons) = HeapRb::<f32>::new(CAP_RING_SIZE).split();
    let started = Arc::new(AtomicBool::new(false));
    let started_recv = Arc::clone(&started);
    let started_play = Arc::clone(&started);
    let empty_cb = Arc::new(AtomicU32::new(0));
    let empty_cb_play = Arc::clone(&empty_cb);
    let empty_cb_recv = Arc::clone(&empty_cb);
    let socket_recv = Arc::clone(&socket);
    let socket_send = Arc::clone(&socket);

    std::thread::spawn(move || {
        let mut decoder = opus::Decoder::new(SAMPLE_RATE, opus::Channels::Mono).unwrap();
        let mut buf = vec![0u8; 65535];
        let mut pcm = vec![0f32; FRAME_SAMPLES];
        loop {
            let n = match socket_recv.recv(&mut buf) {
                Ok(n) => n,
                Err(e) => { tracing::error!("recv: {e}"); continue; }
            };
            let pkt = match parse_packet(&buf[..n]) {
                Some(p) => p,
                None => continue,
            };
            if decoder.decode_float(pkt.opus_payload, &mut pcm, false).is_err() { continue; }
            for &s in &pcm { pb_prod.try_push(s).ok(); }
            empty_cb_recv.store(0, Ordering::Relaxed);
            if !started_recv.load(Ordering::Relaxed) && pb_prod.occupied_len() >= PRIME_SAMPLES {
                started_recv.store(true, Ordering::Relaxed);
                tracing::info!("Buffer primed — starting playback");
            }
        }
    });

    let in_stream = in_device.build_input_stream(
        &in_config,
        move |data: &[f32], _| { for &s in data { cap_prod.try_push(s).ok(); } },
        |e| tracing::error!("Input error: {e}"),
        None,
    )?;

    let out_stream = out_device.build_output_stream(
        &out_config,
        move |data: &mut [f32], _| {
            if !started_play.load(Ordering::Relaxed) { data.fill(0.0); return; }
            let mut last = 0.0f32;
            let mut all_empty = true;
            for chunk in data.chunks_mut(2) {
                let s = match pb_cons.try_pop() {
                    Some(s) => { last = s; all_empty = false; s }
                    None => { last *= UNDERRUN_DECAY; last }
                };
                chunk[0] = s;
                chunk[1] = s;
            }
            handle_underrun(all_empty, &started_play, &empty_cb_play);
        },
        |e| tracing::error!("Output error: {e}"),
        None,
    )?;

    in_stream.play()?;
    out_stream.play()?;

    let mut encoder = make_encoder()?;
    let mut frame = vec![0f32; FRAME_SAMPLES];
    let mut compressed = vec![0u8; 4000];
    let mut seq: u16 = 0;
    let mut ts: u32 = 0;

    tracing::info!("Echo test running — Ctrl+C to stop");
    loop {
        if cap_cons.occupied_len() < FRAME_SAMPLES {
            std::thread::sleep(std::time::Duration::from_micros(200));
            continue;
        }
        for s in frame.iter_mut() { *s = cap_cons.try_pop().unwrap_or(0.0); }
        if let Ok(n) = encoder.encode_float(&frame, &mut compressed) {
            socket_send.send(&build_packet(seq, ts, 0, &compressed[..n])).ok();
        }
        seq = seq.wrapping_add(1);
        ts = ts.wrapping_add(FRAME_SAMPLES as u32);
    }
}

// ─── Entry point ──────────────────────────────────────────────────────────────

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();

    match args.get(1).map(|s| s.as_str()) {
        Some("bidir") => {
            let peer_ip = args
                .get(2)
                .ok_or_else(|| anyhow!(
                    "Usage: bidir [--remote-host IP] [--remote-name NAME] [--channels N] [--token TOKEN] [--id NAME] [--bitrate BPS] [--latency-ms MS] [--fixed-jitter] [--no-phase-lock] [--web ADDR:PORT] [--no-web] [--no-send] [--no-recv]"
                ))?;

            // Support legacy positional remote IP for backward compat with existing configs.
            let remote_host = if !peer_ip.starts_with("--") { peer_ip.as_str() } else { "" };
            let remote_host = {
                let idx = args.iter().position(|a| a == "--remote-host");
                idx.and_then(|i| args.get(i + 1)).map(|s| s.as_str()).unwrap_or(remote_host)
            };

            let remote_device_name = {
                let idx = args.iter().position(|a| a == "--remote-name");
                idx.and_then(|i| args.get(i + 1)).cloned().unwrap_or_default()
            };

            let link_password: Option<String> = {
                let idx = args.iter().position(|a| a == "--link-password");
                idx.and_then(|i| args.get(i + 1)).cloned()
            };

            let num_channels = {
                let idx = args.iter().position(|a| a == "--channels");
                match idx.and_then(|i| args.get(i + 1)) {
                    Some(n) => n.parse::<usize>().map_err(|_| anyhow!("--channels must be 1–{MAX_CHANNELS}"))?,
                    None => 2,
                }
            };

            if args.iter().any(|a| a == "--source") {
                tracing::warn!("--source is deprecated and ignored; transmit audio is controlled by the Web UI routing matrix");
            }
            let source = Source::Matrix;

            let send_enabled = !args.contains(&"--no-send".to_string());
            let recv_enabled = !args.contains(&"--no-recv".to_string());

            let node_id = {
                let idx = args.iter().position(|a| a == "--id");
                match idx.and_then(|i| args.get(i + 1)) {
                    Some(v) => v.clone(),
                    None => default_node_id(),
                }
            };

            let device_name_token = derive_token_from_text(&node_id);

            // Shared token: explicit --token overrides; otherwise derive from remote device name + optional password.
            let shared_token = {
                let idx = args.iter().position(|a| a == "--token");
                match idx.and_then(|i| args.get(i + 1)) {
                    Some(v) => parse_token_arg(v)?,
                    None if !remote_device_name.is_empty() =>
                        derive_link_token(&node_id, &remote_device_name, link_password.as_deref()),
                    None => DEFAULT_SHARED_TOKEN,
                }
            };

            let configured_delay_ms = {
                let idx = args.iter().position(|a| a == "--latency-ms" || a == "--jitter-ms");
                match idx.and_then(|i| args.get(i + 1)) {
                    Some(v) => v.parse::<u32>()
                        .map_err(|_| anyhow!("--latency-ms must be {MIN_LATENCY_MS}–{MAX_LATENCY_MS}"))?
                        .clamp(MIN_LATENCY_MS, MAX_LATENCY_MS),
                    None => 120,
                }
            };
            let target_delay_ms = effective_receive_buffer_ms(configured_delay_ms);

            let jitter = JitterConfig {
                configured_delay_ms,
                target_delay_ms,
                adaptive: !args.contains(&"--fixed-jitter".to_string()),
                phase_lock: !args.contains(&"--no-phase-lock".to_string()),
            };

            let monitor_mode = MonitorMode::PatchMatrix;

            let opus_bitrate_per_channel = {
                let idx = args.iter().position(|a| a == "--bitrate" || a == "--opus-bitrate");
                match idx.and_then(|i| args.get(i + 1)) {
                    Some(v) => v.parse::<u32>().map_err(|_| anyhow!("--bitrate must be a positive integer number of bits/sec per channel"))?,
                    None => 128_000,
                }
            };

            let web_addr = if args.contains(&"--no-web".to_string()) {
                None
            } else {
                let idx = args.iter().position(|a| a == "--web");
                Some(idx.and_then(|i| args.get(i + 1)).cloned().unwrap_or_else(|| "0.0.0.0:8080".to_string()))
            };

            let rendezvous_url = {
                let idx = args.iter().position(|a| a == "--rendezvous");
                idx.and_then(|i| args.get(i + 1)).cloned()
            };

            run_bidir(
                remote_host,
                &remote_device_name,
                num_channels,
                source,
                send_enabled,
                recv_enabled,
                device_name_token,
                shared_token,
                node_id,
                jitter,
                web_addr,
                monitor_mode,
                opus_bitrate_per_channel,
                rendezvous_url,
            )
        }

        Some("send") => {
            let ip = args.get(2).ok_or_else(|| anyhow!("Usage: send <RECEIVER_IP>"))?;
            run_sender(ip)
        }
        Some("recv") => run_receiver(),
        Some("echo") => {
            let ip = args.get(2).ok_or_else(|| anyhow!("Usage: echo <SERVER_IP>"))?;
            run_echo_test(ip)
        }

        Some("help") | Some("--help") | Some("-h") => {
            eprintln!("Usage:");
            eprintln!("  audiolinkd [--web ADDR:PORT] [--id NAME] [--token TOKEN] [--channels N] [--bitrate BPS] [--latency-ms MS]");
            eprintln!("  audiolinkd bidir <REMOTE_IP> [--channels N] [--token TOKEN] [--id NAME] [--bitrate BPS] [--latency-ms MS] [--latency-ms N] [--fixed-jitter] [--no-phase-lock] [--web ADDR:PORT] [--no-web]");
            eprintln!();
            eprintln!("Default mode starts the Web UI first. Configure the remote device in Setup.");
            Ok(())
        }

        _ => {
            let persisted = load_persisted_state().config;
            let node_id = {
                let idx = args.iter().position(|a| a == "--id");
                idx.and_then(|i| args.get(i + 1)).cloned()
                    .or(persisted.node_id.clone())
                    .unwrap_or_else(default_node_id)
            };
            let num_channels = {
                let idx = args.iter().position(|a| a == "--channels");
                match idx.and_then(|i| args.get(i + 1)) {
                    Some(n) => n.parse::<usize>().map_err(|_| anyhow!("--channels must be 1-{MAX_CHANNELS}"))?,
                    None => persisted.channels.unwrap_or(2),
                }.clamp(1, MAX_CHANNELS)
            };
            let remote_device_name = persisted.remote_device_name.clone().unwrap_or_default();
            let link_password = persisted.link_password.clone();
            let shared_token = {
                let idx = args.iter().position(|a| a == "--token");
                match idx.and_then(|i| args.get(i + 1)).cloned().or(persisted.token_hex.clone()) {
                    Some(v) => parse_token_arg(&v)?,
                    None if !remote_device_name.is_empty() =>
                        derive_link_token(&node_id, &remote_device_name, link_password.as_deref()),
                    None => DEFAULT_SHARED_TOKEN,
                }
            };
            let opus_bitrate_per_channel = {
                let idx = args.iter().position(|a| a == "--bitrate" || a == "--opus-bitrate");
                match idx.and_then(|i| args.get(i + 1)) {
                    Some(v) => v.parse::<u32>().map_err(|_| anyhow!("--bitrate must be a positive integer number of bits/sec per channel"))?,
                    None => persisted.opus_bitrate_per_channel.unwrap_or(128_000),
                }
            };
            let configured_delay_ms = {
                let idx = args.iter().position(|a| a == "--latency-ms" || a == "--jitter-ms");
                match idx.and_then(|i| args.get(i + 1)) {
                    Some(v) => v.parse::<u32>().map_err(|_| anyhow!("--latency-ms must be {MIN_LATENCY_MS}-{MAX_LATENCY_MS}"))?.clamp(MIN_LATENCY_MS, MAX_LATENCY_MS),
                    None => persisted.latency_ms.unwrap_or(120).clamp(MIN_LATENCY_MS, MAX_LATENCY_MS),
                }
            };
            let target_delay_ms = effective_receive_buffer_ms(configured_delay_ms);
            let jitter = JitterConfig {
                configured_delay_ms,
                target_delay_ms,
                adaptive: !args.contains(&"--fixed-jitter".to_string()) && !persisted.fixed_jitter.unwrap_or(false),
                phase_lock: !args.contains(&"--no-phase-lock".to_string()) && persisted.phase_lock.unwrap_or(true),
            };
            let web_addr = {
                let idx = args.iter().position(|a| a == "--web");
                idx.and_then(|i| args.get(i + 1)).cloned().unwrap_or_else(|| "0.0.0.0:8080".to_string())
            };
            let remote_host = {
                let idx = args.iter().position(|a| a == "--remote" || a == "--device" || a == "--remote-host");
                idx.and_then(|i| args.get(i + 1)).cloned().or(persisted.remote.clone())
            };
            let rendezvous_url = {
                let idx = args.iter().position(|a| a == "--rendezvous");
                idx.and_then(|i| args.get(i + 1)).cloned()
                    .or(persisted.rendezvous_url.clone())
                    .filter(|u| !u.trim().is_empty())
            };

            if let Some(remote_host) = remote_host.filter(|r| !r.trim().is_empty()) {
                tracing::info!("AudioLink loading persistent config: remote_host={remote_host} remote_device={remote_device_name}");
                run_bidir(
                    &remote_host,
                    &remote_device_name,
                    num_channels,
                    Source::Matrix,
                    true,
                    true,
                    derive_token_from_text(&node_id),
                    shared_token,
                    node_id,
                    jitter,
                    Some(web_addr),
                    MonitorMode::PatchMatrix,
                    opus_bitrate_per_channel,
                    rendezvous_url,
                )
            } else if !remote_device_name.is_empty() {
                tracing::info!("AudioLink starting in responder mode: waiting for {remote_device_name}");
                run_bidir(
                    "",
                    &remote_device_name,
                    num_channels,
                    Source::Matrix,
                    true,
                    true,
                    derive_token_from_text(&node_id),
                    shared_token,
                    node_id,
                    jitter,
                    Some(web_addr),
                    MonitorMode::PatchMatrix,
                    opus_bitrate_per_channel,
                    rendezvous_url,
                )
            } else {
                let state = control_web_state(node_id.clone(), shared_token, num_channels, opus_bitrate_per_channel, jitter)?;
                tracing::info!("AudioLink Web UI starting — open http://{web_addr} and use Setup to connect");
                spawn_web_ui(web_addr, state);
                loop { std::thread::sleep(Duration::from_secs(3600)); }
            }
        }
    }
}
