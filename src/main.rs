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
// If the sender engine restarts, its RTP timestamp/sequence counters start
// again from the beginning. A high-watermark intended to drop genuinely late
// packets must not reject the new stream until it catches up minutes later.
const RTP_RESTART_ROLLBACK_SAMPLES: u32 = SAMPLE_RATE; // 1 second at 48kHz
const RTP_RESTART_LOW_TS_SAMPLES: u32 = SAMPLE_RATE * 2; // first 2s of a fresh sender
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

// ─── Encoder mode ─────────────────────────────────────────────────────────────

/// Selects the Opus application type used for the send encoder.
///
/// Music  → Application::Audio (CELT engine, no FEC).
///          Best for wideband programme material. Maximises quality per bit
///          at the cost of no packet-loss recovery beyond Opus's own concealment.
///
/// Speech → Application::Voip (SILK + CELT hybrid, inband FEC enabled).
///          Embeds a lower-bitrate redundant copy of each frame in the next
///          packet. If that packet arrives, the decoder reconstructs the lost
///          frame from the embedded data — effectively halving perceived loss
///          rate on a flaky mobile path. The bitrate overhead is ~30–40%.
///          The decoder requires no changes; FEC is transparent to the receiver.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EncoderMode {
    #[default]
    Music,
    Speech,
}

impl EncoderMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Music  => "music",
            Self::Speech => "speech",
        }
    }

    pub fn parse(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "speech" | "voice" | "voip" => Self::Speech,
            _ => Self::Music,
        }
    }
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

fn make_encoder_for_mode(bitrate_per_channel: u32, mode: EncoderMode) -> Result<opus::Encoder> {
    let app = match mode {
        EncoderMode::Music  => opus::Application::Audio,
        EncoderMode::Speech => opus::Application::Voip,
    };
    let mut enc = opus::Encoder::new(SAMPLE_RATE, opus::Channels::Mono, app)?;
    enc.set_bitrate(opus::Bitrate::Bits(bitrate_per_channel as i32))?;
    // FEC is only meaningful in Voip/Speech mode. In Audio mode the encoder
    // ignores the FEC flag but we set it explicitly for clarity.
    enc.set_inband_fec(matches!(mode, EncoderMode::Speech))?;
    Ok(enc)
}

fn make_encoder_with_bitrate(bitrate_per_channel: u32) -> Result<opus::Encoder> {
    make_encoder_for_mode(bitrate_per_channel, EncoderMode::default())
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

fn fresh_opus_decoders() -> Vec<opus::Decoder> {
    (0..MAX_CHANNELS)
        .map(|_| opus::Decoder::new(SAMPLE_RATE, opus::Channels::Mono).unwrap())
        .collect()
}

fn fresh_asrc_resamplers() -> Vec<FastFixedIn<f32>> {
    (0..MAX_CHANNELS)
        .map(|_| {
            FastFixedIn::new(
                1.0,
                1.01,
                PolynomialDegree::Linear,
                FRAME_SAMPLES,
                1,
            )
            .expect("Rubato resampler init failed")
        })
        .collect()
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

fn rtp_timestamp_at_or_before(ts: u32, reference: u32) -> bool {
    // RFC3550-style serial-number comparison. True for equal timestamps and
    // timestamps behind `reference`, false for timestamps ahead of it.
    reference.wrapping_sub(ts) < 0x8000_0000
}

fn rtp_seq_looks_reset(prev: u16, next: u16) -> bool {
    // Treat a backwards sequence jump as a probable sender restart, while not
    // flagging the normal 65535 -> 0 wrap as backwards. This is only used in
    // combination with a large RTP timestamp rollback, so small reordering does
    // not reset the stream.
    prev != next && prev.wrapping_sub(next) < 0x8000
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

fn udp_disconnect_socket(socket: &std::net::UdpSocket) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;

        unsafe {
            let mut sa: libc::sockaddr_storage = std::mem::zeroed();
            #[cfg(any(
                target_os = "macos",
                target_os = "ios",
                target_os = "freebsd",
                target_os = "openbsd",
                target_os = "netbsd",
                target_os = "dragonfly"
            ))]
            {
                sa.ss_len = std::mem::size_of::<libc::sockaddr>() as u8;
            }
            sa.ss_family = libc::AF_UNSPEC as libc::sa_family_t;

            // Linux accepts either sockaddr or sockaddr_storage length here; BSD/macOS
            // is fussier, so try the small sockaddr length first, then the storage
            // length. Calling AF_UNSPEC on an already-unconnected UDP socket is okay.
            let ptr = &sa as *const libc::sockaddr_storage as *const libc::sockaddr;
            let rc = libc::connect(
                socket.as_raw_fd(),
                ptr,
                std::mem::size_of::<libc::sockaddr>() as libc::socklen_t,
            );
            if rc == 0 {
                return Ok(());
            }

            let _first_err = std::io::Error::last_os_error();
            let rc = libc::connect(
                socket.as_raw_fd(),
                ptr,
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            );
            if rc == 0 {
                Ok(())
            } else {
                let err = std::io::Error::last_os_error();
                if err.kind() == std::io::ErrorKind::NotConnected {
                    Ok(())
                } else {
                    Err(err)
                }
            }
        }
    }

    #[cfg(not(unix))]
    {
        let _ = socket;
        Ok(())
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
) -> (usize, usize, Option<u32>) {
    let active = remote_channels.min(MAX_CHANNELS);
    if active == 0 {
        return (0, 0, None);
    }

    // Drain strictly oldest-first. A complete oldest group is ready immediately.
    // An incomplete oldest group becomes ready after the 10ms phase-lock timeout
    // and missing/corrupt channels are generated with Opus PLC.
    // Returns the last drained RTP timestamp so the caller can discard any
    // late-arriving packets for already-processed timestamps — preventing
    // double-decode which inflates decoded_fps above 50 and drives fill growth.
    let mut output_groups = 0usize;
    let mut plc_channels = 0usize;
    let mut last_drained_ts: Option<u32> = None;

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
        last_drained_ts = Some(ready_ts);
    }

    (output_groups, plc_channels, last_drained_ts)
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
    encoder_mode: Option<EncoderMode>,
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
    /// P95 inter-packet arrival jitter over a rolling 60s window (~300 packets at 50fps).
    /// More honest than the EMA for buffer-sizing decisions — EMA smooths spikes out.
    jitter_p95_ms: f64,
    /// Suggested receive buffer: jitter_p95 × 4, clamped to 500ms.
    /// Displayed as a non-blocking UI hint when it meaningfully exceeds the current target.
    recommended_buffer_ms: u32,
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
    /// "music" or "speech" — the Opus Application mode in use for the send encoder.
    encoder_mode: String,
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
    /// Whether to hold all channels for a given RTP timestamp until the complete
    /// set arrives before decoding. Defaults to current runtime value if absent.
    phase_lock: Option<bool>,
    /// Opus encoder application mode: "music" (Application::Audio, no FEC) or
    /// "speech" (Application::Voip, inband FEC enabled). Defaults to current value.
    encoder_mode: Option<String>,
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
            label: format!("{device_name} • {label}"),
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
}async fn index_handler(State(_state): State<WebState>) -> Html<String> {
    Html(r##"<!doctype html>
<html lang="en-GB">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AudioLink Control</title>
<style>
:root{--bg:#0d1117;--panel:#161c24;--panel2:#1c2430;--ink:#e2eaf2;--muted:#7e8fa0;--line:#252f3a;--cell:#1a2430;--green:#1dba5b;--orange:#f0a030;--red:#e04040;--blue:#4d9fff;--blue2:#1e5fa0;--hbg:#090d12;--hborder:#1e2830;--nbg:#0f141a;--logo:#e8eef4;--htext:#7e8fa0;--mono:'JetBrains Mono','Fira Code','Cascadia Code',monospace}
body.light{--bg:#f0f3f7;--panel:#fff;--panel2:#e6eaf0;--ink:#111820;--muted:#6b7a8d;--line:#ccd3de;--cell:#dde3ec;--blue2:#1858a8;--hbg:#fff;--hborder:#ccd3de;--nbg:#f5f7fa;--logo:#111820;--htext:#5a6a7a}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);font:13.5px/1.5 'IBM Plex Sans','Segoe UI',system-ui,sans-serif;transition:background .2s,color .2s}
input,select,button{font:inherit;background:var(--bg);color:var(--ink);border:1px solid var(--line);border-radius:7px;padding:7px 10px;transition:background .2s,color .2s,border-color .2s}
input:focus,select:focus{outline:2px solid var(--blue);outline-offset:1px;border-color:transparent}
button{cursor:pointer}
header{display:flex;align-items:center;justify-content:space-between;padding:11px 20px;border-bottom:1px solid var(--hborder);background:var(--hbg);position:sticky;top:0;z-index:20;gap:10px;transition:background .2s,border-color .2s}
.logo{font-size:15px;font-weight:700;letter-spacing:.12em;text-transform:uppercase;color:var(--logo);flex-shrink:0;transition:color .2s}
.logo span{color:var(--blue)}
.nodeline{color:var(--htext);font-size:11px;margin-top:2px;font-family:var(--mono);transition:color .2s}
.hright{display:flex;align-items:center;gap:9px;flex-shrink:0}
.theme-btn{background:transparent;border:1px solid var(--hborder);border-radius:16px;padding:4px 10px;font-size:11px;color:var(--htext);transition:border-color .2s,color .2s}
.theme-btn:hover{border-color:var(--muted);color:var(--ink)}
.conn-badge{display:inline-flex;align-items:center;gap:6px;padding:5px 11px;border-radius:18px;font-size:12px;font-weight:600;border:1px solid;white-space:nowrap}
.conn-badge.green{background:rgba(29,186,91,.1);border-color:rgba(29,186,91,.4);color:#3ed47a}
.conn-badge.orange{background:rgba(240,160,48,.1);border-color:rgba(240,160,48,.4);color:#e8920a}
.conn-badge.gray{background:rgba(100,120,140,.07);border-color:rgba(100,120,140,.22);color:var(--muted)}
.lamp{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.lamp.green{background:var(--green);box-shadow:0 0 6px var(--green)}
.lamp.orange{background:var(--orange);box-shadow:0 0 6px var(--orange)}
.lamp.gray{background:#3a4a5a}
body.light .lamp.gray{background:#a0b0c0}
nav{display:flex;gap:5px;padding:8px 20px;background:var(--nbg);border-bottom:1px solid var(--line);position:sticky;top:51px;z-index:19;overflow-x:auto;transition:background .2s,border-color .2s}
.tab{padding:5px 13px;border-radius:7px;font-size:13px;font-weight:500;border:1px solid transparent;background:transparent;color:var(--muted);white-space:nowrap;transition:.12s}
.tab:hover{color:var(--ink);background:var(--panel)}
.tab.active{background:var(--blue2);color:#c8e0ff;border-color:rgba(77,159,255,.28)}
body.light .tab.active{color:#fff}
.alertbar{display:none;padding:8px 20px;font-size:12px;font-weight:700;letter-spacing:.04em;text-align:center}
.alertbar.show{display:block}
.alertbar.offline{background:#6e1010;color:#ffc8c8}
.alertbar.degraded{background:#6a3c08;color:#ffd8a0}
main{padding:16px 20px;max-width:1400px;margin:auto}
.page{display:none}.page.active{display:block}
.mb{margin-bottom:13px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:13px;padding:15px;transition:background .2s,border-color .2s}
.ctitle{font-size:10px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:var(--muted);margin-bottom:12px}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:13px}
@media(max-width:700px){.g2{grid-template-columns:1fr}}
.link-ov{display:grid;grid-template-columns:1fr 1px 1fr;gap:16px;align-items:start}
.node-col{display:flex;flex-direction:column;gap:5px}
.nname{font-size:19px;font-weight:700;font-family:var(--mono)}
.nsub-chip{background:var(--panel2);border:1px solid var(--line);border-radius:5px;padding:2px 7px;font-size:11px;color:var(--muted);white-space:nowrap}
.nsub-chip.live{color:var(--green);border-color:rgba(29,186,91,.3)}
.divv{background:var(--line);min-height:60px;align-self:stretch}
.statgrid{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin-bottom:13px}
@media(max-width:780px){.statgrid{grid-template-columns:repeat(3,1fr)}}
.stile{background:var(--panel2);border:1px solid var(--line);border-radius:10px;padding:12px 13px;transition:background .2s}
.slbl{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.07em;margin-bottom:6px}
.sval{font-size:20px;font-weight:700;font-family:var(--mono);line-height:1}
.sval.ok{color:var(--green)}.sval.warn{color:var(--orange)}.sval.bad{color:var(--red)}.sval.dim{color:var(--ink)}
.sval .u{font-size:12px;font-weight:400;color:var(--muted)}
.bufrow{display:flex;align-items:baseline;gap:8px;margin-bottom:9px}
.bufms{font-size:22px;font-weight:700;font-family:var(--mono)}
.bufmeta{font-size:12px;color:var(--muted)}
.buftrack{height:10px;background:var(--bg);border-radius:5px;position:relative;border:1px solid var(--line);overflow:visible;transition:background .2s}
.buffill{height:100%;border-radius:5px;background:var(--green);transition:width .22s ease}
.buffill.warn{background:var(--orange)}.buffill.bad{background:var(--red)}
.buftick{position:absolute;top:-4px;bottom:-4px;width:2px;background:rgba(128,160,180,.5);border-radius:2px}
.bufleg{display:flex;justify-content:space-between;font-size:10px;color:var(--muted);margin-top:4px}
details{margin-top:9px}
details summary{cursor:pointer;list-style:none;font-size:12px;color:var(--blue);padding:4px 0;display:flex;align-items:center;gap:5px;user-select:none}
details summary::-webkit-details-marker{display:none}
details summary::before{content:'›';font-size:15px;transition:.12s;display:inline-block;width:12px}
details[open] summary::before{transform:rotate(90deg)}
.dkv{display:grid;grid-template-columns:1fr 1fr;gap:5px 13px;padding:10px;background:var(--bg);border-radius:8px;margin-top:5px;border:1px solid var(--line);font-size:12px;transition:background .2s}
.dkv .k{color:var(--muted)}.dkv .v{font-family:var(--mono);font-size:11px}
.mbank{display:flex;flex-wrap:wrap;gap:16px;align-items:flex-end;padding:4px 0}
.meter{display:flex;flex-direction:column;align-items:center;gap:5px;flex-shrink:0}
.bartrack{width:22px;height:220px;position:relative;border-radius:3px;border:1px solid var(--line);overflow:hidden;background:var(--bg);transition:background .2s,border-color .2s}
.bartrack::after{content:'';position:absolute;inset:0;pointer-events:none;z-index:3;background:linear-gradient(to top,transparent calc(70% - .5px),rgba(255,255,255,.2) calc(70% - .5px),rgba(255,255,255,.2) calc(70% + .5px),transparent calc(70% + .5px)),linear-gradient(to top,transparent calc(83.3% - .5px),rgba(255,255,255,.15) calc(83.3% - .5px),rgba(255,255,255,.15) calc(83.3% + .5px),transparent calc(83.3% + .5px))}body.light .bartrack::after{background:linear-gradient(to top,transparent calc(70% - .5px),rgba(0,0,0,.25) calc(70% - .5px),rgba(0,0,0,.25) calc(70% + .5px),transparent calc(70% + .5px)),linear-gradient(to top,transparent calc(83.3% - .5px),rgba(0,0,0,.2) calc(83.3% - .5px),rgba(0,0,0,.2) calc(83.3% + .5px),transparent calc(83.3% + .5px))}
.sg,.so,.sr{position:absolute;left:0;right:0;transition:height .05s linear,bottom .05s linear}
.sg{background:var(--green);bottom:0;z-index:1}.so{background:var(--orange);z-index:2}.sr{background:var(--red);z-index:2}
.barticks{position:absolute;right:calc(100% + 5px);top:0;bottom:0;width:26px;pointer-events:none}
.tlbl{position:absolute;font-size:9px;color:var(--muted);right:0;transform:translateY(-50%);font-family:var(--mono);line-height:1;white-space:nowrap}
.tlbl.hi{color:var(--ink)}
.mdb{font-family:var(--mono);font-size:10px;color:var(--muted);width:40px;text-align:center}
.mlbl{font-size:10px;color:var(--muted);text-align:center;width:44px;line-height:1.3}
.page.routing{margin:-16px -20px 0}
.route-toolbar{display:flex;align-items:center;gap:10px;padding:9px 20px;background:var(--nbg);border-bottom:1px solid var(--line);transition:background .2s}
.route-btn{padding:5px 14px;font-size:12px;font-weight:600;background:var(--panel);border:1px solid var(--line);border-radius:7px;color:var(--muted);transition:color .15s,border-color .15s,background .15s}
.route-btn:hover{color:var(--ink);border-color:var(--muted)}
.route-btn.arm{background:rgba(224,64,64,.1);border-color:rgba(224,64,64,.5);color:var(--red);animation:armPulse .25s ease}
@keyframes armPulse{from{transform:scale(1.04)}to{transform:scale(1)}}
.matrix-outer{overflow:auto;max-height:calc(100vh - 148px);border-bottom:1px solid var(--line)}
table.mx{border-collapse:separate;border-spacing:0;table-layout:fixed}
.mx th,.mx td{border-right:1px solid var(--line);border-bottom:1px solid var(--line)}
.mx td.rh,.mx th.rh{position:sticky;left:0;z-index:2;background:var(--panel2);width:230px;min-width:230px;padding:0;transition:background .2s}
.mx thead th.rh{z-index:5;top:0}
.mx thead th:not(.rh){position:sticky;top:0;z-index:3;background:var(--panel2);width:60px;min-width:60px;max-width:60px;padding:0;vertical-align:bottom;transition:background .2s}
.ch-hdr{display:flex;flex-direction:column;align-items:center;padding:6px 2px 5px;gap:2px;min-height:56px;justify-content:flex-end}
.ch-hdr-num{font-size:11px;font-weight:700;color:var(--muted);letter-spacing:.04em}
.ch-hdr-lbl{font-size:11px;color:var(--ink);font-weight:600;width:56px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;text-align:center;min-height:12px}
.src-row{display:flex;align-items:center;gap:5px;padding:0 8px;height:36px;white-space:nowrap;overflow:hidden}
.rsig{display:inline-block;width:7px;height:7px;border-radius:50%;background:#2e3e4e;flex-shrink:0;transition:background .15s}
.rsig.green{background:var(--green);box-shadow:0 0 4px var(--green)}
.rsig.orange{background:var(--orange)}
.rsig.red{background:var(--red)}
.src-sysname{font-size:13px;font-weight:600;flex-shrink:0;overflow:hidden;text-overflow:ellipsis}
.src-sep{color:var(--muted);font-size:11px;flex-shrink:0}
.src-label-input{flex:1;min-width:40px;max-width:120px;padding:1px 5px;font-size:11px;font-weight:600;background:transparent;border:1px solid transparent;border-radius:5px;color:var(--ink);transition:border-color .12s,background .12s}
.src-label-input:focus{background:var(--panel);border-color:var(--blue);outline:none}
.src-label-input.dirty{border-color:var(--orange)}
.src-label-input::placeholder{color:var(--line)}
.mx tr.src-spacer td{height:10px;background:var(--bg);border-right:none;pointer-events:none}
.mx tr.src-spacer td.rh{background:var(--bg)}
.xcell{width:60px;min-width:60px;max-width:60px;height:60px;background:var(--cell);cursor:pointer;padding:0;transition:filter .06s}
.xcell.on{background:var(--green)}
.mx tr.hl-row > .rh{background:rgba(77,159,255,.12)!important}
.mx tr.hl-row > .xcell:not(.on){background:rgba(77,159,255,.10)!important}
.mx td.hl-col.xcell:not(.on){background:rgba(77,159,255,.08)!important}
.mx th.hl-col{background:rgba(77,159,255,.08)!important}
.mx tr.hl-row > td.hl-col.xcell:not(.on){background:rgba(77,159,255,.20)!important}
.mx .xcell.on.hl-col,.mx tr.hl-row > .xcell.on{filter:brightness(1.3)}
.sgrid{display:grid;grid-template-columns:1fr 1fr;gap:13px;align-items:start}
@media(max-width:860px){.sgrid{grid-template-columns:1fr}}
.field{display:grid;gap:5px;margin-bottom:10px}
.field label{font-size:12px;font-weight:600}
.hint{font-weight:400;color:var(--muted)}
.field input,.field select{width:100%}
.idrow{display:flex;gap:6px}
.idrow input{flex:1;font-family:var(--mono);font-size:13px;font-weight:600}
.cpybtn{padding:7px 10px;font-size:12px;background:var(--panel2);border-color:var(--line);white-space:nowrap;flex-shrink:0}
.savebtn{padding:7px 10px;font-size:12px;background:var(--blue2);border-color:rgba(77,159,255,.38);color:#c8e0ff;white-space:nowrap;flex-shrink:0;display:none}
.savebtn.show{display:inline-block}
.mpill{display:inline-flex;align-items:center;gap:7px;padding:6px 13px;border-radius:18px;font-size:12px;font-weight:700;border:1px solid;transition:.2s;margin-top:11px}
.mpill.rdv{background:rgba(77,159,255,.09);border-color:rgba(77,159,255,.32);color:#5ab0f0}
.mpill.dir{background:rgba(29,186,91,.09);border-color:rgba(29,186,91,.32);color:#3dcc72}
.mpill.pas{background:rgba(100,120,140,.06);border-color:rgba(100,120,140,.22);color:var(--muted)}
.mdot{width:7px;height:7px;border-radius:50%;flex-shrink:0;background:currentColor}
.crow{display:flex;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--line)}
.crow:last-child{border-bottom:none}
.ck{font-size:12px;color:var(--muted)}
.cv select{background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:4px 8px;font-size:12px}
.cv input[type=checkbox]{width:auto;margin:0;accent-color:var(--green)}
.applybtn{width:100%;padding:8px 11px;margin-top:10px;background:var(--blue2);border-color:rgba(77,159,255,.38);color:#c8e0ff;font-size:12px;font-weight:600;border-radius:8px;transition:background .15s,border-color .15s,color .15s}
.applybtn:hover{background:#2870c4}
.applybtn.arm{background:rgba(224,64,64,.14);border-color:rgba(224,64,64,.5);color:var(--red)}
.applybtn.done{background:rgba(29,186,91,.14);border-color:rgba(29,186,91,.4);color:var(--green);cursor:default}
.devrow{display:flex;justify-content:space-between;padding:7px 0;border-bottom:1px solid var(--line);font-size:13px}
.devrow:last-child{border-bottom:none}
.dk{color:var(--muted)}
.card-divider{border:none;border-top:1px solid var(--line);margin:12px 0}
</style>
</head>
<body>
<div id="alertbar" class="alertbar offline show">REMOTE DEVICE OFFLINE</div>
<header>
  <div><div class="logo">Audio<span>Link</span></div><div class="nodeline" id="nodeLine">Starting…</div></div>
  <div class="hright">
    <button class="theme-btn" id="themeBtn" onclick="toggleTheme()"></button>
    <div id="connBadge" class="conn-badge gray"><span class="lamp gray" id="peerLamp"></span><span id="peerText">No connected device</span></div>
  </div>
</header>
<nav>
  <button class="tab active" data-page="home">Home</button>
  <button class="tab" data-page="txrouting">Send Routing</button>
  <button class="tab" data-page="rxrouting">Receive Routing</button>
  <button class="tab" data-page="meters">Meters</button>
  <button class="tab" data-page="setup">Setup</button>
</nav>
<main>
<section id="home" class="page active">
  <div class="card mb">
    <div class="ctitle">Link</div>
    <div class="link-ov">
      <div class="node-col">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px">This device</div>
        <div class="nname" id="localName">—</div>
        <div style="display:flex;flex-wrap:wrap;gap:5px;margin-top:6px">
          <span class="nsub-chip" id="localCh">—</span>
          <span class="nsub-chip" id="localBr">—</span>
          <span class="nsub-chip" id="localCodec">music</span>
          <span class="nsub-chip" id="localPl">phase lock</span>
        </div>
      </div>
      <div class="divv"></div>
      <div class="node-col">
        <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px">Remote device</div>
        <div class="nname" id="remoteNameHome" style="color:var(--muted)">—</div>
        <div id="remoteChips" style="display:flex;flex-wrap:wrap;gap:5px;margin-top:6px"></div>
      </div>
    </div>
  </div>
  <div class="statgrid">
    <div class="stile"><div class="slbl">One-way latency</div><div class="sval ok" id="owVal">—<span class="u"> ms</span></div></div>
    <div class="stile"><div class="slbl">TX</div><div class="sval dim"><span id="txBw">—</span><span class="u"> Mb/s</span></div></div>
    <div class="stile"><div class="slbl">RX</div><div class="sval dim"><span id="rxBw">—</span><span class="u"> Mb/s</span></div></div>
    <div class="stile"><div class="slbl">Packet loss</div><div class="sval ok" id="lossVal">—<span class="u"> %</span></div></div>
    <div class="stile"><div class="slbl">Jitter EMA</div><div class="sval ok" id="jitterVal">—<span class="u"> ms</span></div></div>
  </div>
  <div class="card mb">
    <div class="ctitle">Incoming Buffer</div>
    <div class="bufrow"><span class="bufms" id="fillMs">—</span><span class="bufmeta">ms · target <b id="targetMs" style="color:var(--ink)">—</b> ms</span></div>
    <div class="buftrack"><div class="buffill" id="bufFill" style="width:0"></div><div class="buftick" id="bufTick" style="left:100%"></div></div>
    <div class="bufleg" id="bufLeg" style="position:relative;height:14px"><span style="position:absolute;left:0">0</span><span id="bufTickLbl" style="position:absolute;transform:translateX(-50%)">—</span><span id="bufLegMax" style="position:absolute;right:0">— ms</span></div>
  </div>
  <div class="card">
    <div class="ctitle">Transport</div>
    <div style="font-size:13px;color:var(--muted)">
      <div>Mode: <b id="tMode" style="color:var(--ink)">—</b></div>
      <div>Remote host: <b id="tHost" style="color:var(--ink)">—</b></div>
    </div>
    <details>
      <summary>Diagnostics</summary>
      <div class="dkv">
        <span class="k">RTT</span><span class="v" id="dRtt">—</span>
        <span class="k">One-way est.</span><span class="v" id="dOwLat">—</span>
        <span class="k">Clock drift</span><span class="v" id="dDrift">—</span>
        <span class="k">Decoded fps</span><span class="v" id="dFps">—</span>
        <span class="k">TX fps</span><span class="v" id="dTxFps">—</span>
        <span class="k">Queued groups</span><span class="v" id="dQ">—</span>
        <span class="k">Ring overflows</span><span class="v" id="dOv">—</span>
        <span class="k">PLC channels</span><span class="v" id="dPlc">—</span>
        <span class="k">Seq missing</span><span class="v" id="dMiss">—</span>
        <span class="k">Suggested buffer</span><span class="v" id="dSug">—</span>
        <span class="k">Underflows</span><span class="v" id="dUf">—</span>
        <span class="k">Uptime</span><span class="v" id="dUp">—</span>
      </div>
    </details>
  </div>
</section>
<section id="txrouting" class="page routing">
  <div class="route-toolbar">
    <button class="route-btn" id="txBtn1to1" onclick="routeConfirm('txBtn1to1',doTx1to1)">1:1</button>
    <button class="route-btn" id="txBtnClear" onclick="routeConfirm('txBtnClear',doTxClear)">Clear all</button>
    <button class="route-btn" id="txBtnResetLabels" onclick="routeConfirm('txBtnResetLabels',doResetLabels)" style="margin-left:auto">Reset labels</button>
  </div>
  <div class="matrix-outer"><table class="mx" id="txTable"><thead id="txHead"></thead><tbody id="txBody"></tbody></table></div>
</section>
<section id="rxrouting" class="page routing">
  <div class="route-toolbar">
    <button class="route-btn" id="rxBtn1to1" onclick="routeConfirm('rxBtn1to1',doRx1to1)">1:1</button>
    <button class="route-btn" id="rxBtnClear" onclick="routeConfirm('rxBtnClear',doRxClear)">Clear all</button>
  </div>
  <div class="matrix-outer"><table class="mx" id="rxTable"><thead id="rxHead"></thead><tbody id="rxBody"></tbody></table></div>
</section>
<section id="meters" class="page">
  <div class="card mb"><div class="ctitle">Send</div><div class="mbank" id="txMeters"></div></div>
  <div class="card mb"><div class="ctitle">Receive</div><div class="mbank" id="rxMeters"></div></div>
  <div class="card"><div class="ctitle">Output</div><div class="mbank" id="monMeters"></div></div>
</section>
<section id="setup" class="page">
  <div class="sgrid mb">
    <div class="card">
      <div class="ctitle">This Node</div>
      <div class="field">
        <label>Device name</label>
        <div class="idrow">
          <input id="cfgNode" autocomplete="off" spellcheck="false" oninput="nodeNameDirty()">
          <button class="cpybtn" onclick="copyFeedback(this,$('cfgNode').value)">Copy</button>
          <button class="savebtn" id="nodeNameSave" onclick="saveNodeName()">Save</button>
        </div>
      </div>
      <div class="field" style="margin-bottom:0">
        <label>Device IP</label>
        <div class="idrow">
          <input id="deviceIpInput" readonly style="flex:1;color:var(--muted)">
          <button class="cpybtn" onclick="copyFeedback(this,$('deviceIpInput').value)">Copy</button>
        </div>
      </div>
    </div>
    <div class="card">
      <div class="ctitle">Connect To</div>
      <div class="field"><label>Remote device name</label><input id="cfgRemoteName" autocomplete="off" spellcheck="false" oninput="updateMode();cfgDirty.remoteName=true" style="font-family:var(--mono);font-size:13px;font-weight:600"></div>
      <div class="field" style="margin-bottom:0"><label>Remote IP</label><input id="cfgPeer" autocomplete="off" spellcheck="false" oninput="updateMode();cfgDirty.peer=true" style="font-family:var(--mono);font-size:13px;font-weight:600"></div>
    </div>
  </div>
  <div class="sgrid mb" style="align-items:stretch">
    <div class="card">
      <div class="ctitle">Codec &amp; Transport</div>
      <div class="crow"><span class="ck">Network send channels</span><span class="cv"><select id="cfgChannels" onchange="cfgDirty.channels=true"><option>1</option><option>2</option><option>4</option><option>6</option><option>8</option><option>16</option><option>24</option><option>32</option><option>40</option><option>64</option></select></span></div>
      <div class="crow"><span class="ck">Bitrate per channel</span><span class="cv"><select id="bitrate" onchange="cfgDirty.bitrate=true"><option value="32000">32 kb/s</option><option value="48000">48 kb/s</option><option value="64000">64 kb/s</option><option value="96000">96 kb/s</option><option value="128000">128 kb/s</option><option value="192000">192 kb/s</option><option value="256000">256 kb/s</option></select></span></div>
      <div class="crow"><span class="ck">Incoming buffer</span><span class="cv"><select id="rxBuffer" onchange="cfgDirty.rxBuffer=true"><option value="5">5 ms</option><option value="20">20 ms</option><option value="40">40 ms</option><option value="60">60 ms</option><option value="80">80 ms</option><option value="100">100 ms</option><option value="120">120 ms</option><option value="160">160 ms</option><option value="200">200 ms</option><option value="300">300 ms</option><option value="500">500 ms</option><option value="1000">1 s</option><option value="2000">2 s</option></select></span></div>
      <div class="crow"><span class="ck">Encoder</span><span class="cv"><select id="cfgEncoderMode" onchange="cfgDirty.encoderMode=true"><option value="music">Music</option><option value="speech">Voice</option></select></span></div>
      <div class="crow"><span class="ck">Phase lock</span><span class="cv"><input type="checkbox" id="cfgPhaseLock" checked onchange="cfgDirty.phaseLock=true"></span></div>
      <button class="applybtn" id="applyBtn" onclick="applySetup()">Apply &amp; rebuild engine</button>
    </div>
    <div class="card">
      <div class="ctitle">Advanced</div>
      <div id="modePill" class="mpill rdv" style="margin-top:0;margin-bottom:14px"><span class="mdot"></span><span id="modeLabel">Cloud</span></div>
      <div class="field">
        <label>Link password</label>
        <div style="display:flex;gap:6px"><input id="cfgLinkPw" type="password" style="flex:1" oninput="cfgDirty.linkPw=true"><button id="pwToggleBtn" onclick="togglePw()" style="padding:7px 12px;flex-shrink:0;min-width:52px">Show</button></div>
      </div>
      <div class="field"><label>Token override</label><input id="cfgToken" autocomplete="off" spellcheck="false"></div>
      <div class="field" style="margin-bottom:0"><label>Rendezvous server</label><input id="cfgRendezvous" value="https://audiolink.amsound.co.uk" autocomplete="off" oninput="cfgDirty.rendezvous=true"></div>
    </div>
  </div>
  <div class="card"><div class="ctitle">Audio Devices</div><div id="devices"><div style="color:var(--muted);font-size:12px">Loading…</div></div></div>
</section>
</main>
<script>
const $=id=>document.getElementById(id);
const esc=s=>s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
// Tab routing
document.querySelectorAll('.tab').forEach(b=>b.onclick=()=>{document.querySelectorAll('.tab,.page').forEach(x=>x.classList.remove('active'));b.classList.add('active');$(b.dataset.page).classList.add('active')});
// Theme
const mq=window.matchMedia('(prefers-color-scheme: dark)');
let dark;
function applyTheme(isDark,save){dark=isDark;document.body.classList.toggle('light',!isDark);$('themeBtn').textContent=isDark?'☀ Light':'☾ Dark';if(save)localStorage.setItem('al-theme',isDark?'dark':'light')}
const savedTheme=localStorage.getItem('al-theme');
applyTheme(savedTheme?savedTheme==='dark':mq.matches,false);
mq.addEventListener('change',e=>{if(!localStorage.getItem('al-theme'))applyTheme(e.matches,false)});
function toggleTheme(){applyTheme(!dark,true)}
// App state
let status={},stats={},matrix={sources:[],destinations:[],routes:[]},devices={};
let cfgDirty={};
let savedNodeName='';
let matrixLastKey='';
let routeBusy=false;
const armTimers={};
// srcUserLabels: user-assigned labels keyed by source endpoint id e.g. "input:0"
// This is the source of truth on the client. Persists across matrix rebuilds.
const srcUserLabels={};
// Helpers
function setSelectValue(id,value){const el=$(id);if(!el)return;const v=String(value);if([...el.options].some(o=>o.value===v||o.text===v))el.value=v}
function copyFeedback(btn,text){function done(){btn.textContent='Copied!';setTimeout(()=>btn.textContent='Copy',1800)}function fail(){btn.textContent='Failed';setTimeout(()=>btn.textContent='Copy',1800)}function execCopy(){try{const t=document.createElement('textarea');t.value=text;t.style.cssText='position:fixed;opacity:0;top:0;left:0';document.body.appendChild(t);t.focus();t.select();document.execCommand('copy');document.body.removeChild(t);done()}catch(e){fail()}}if(navigator.clipboard&&window.isSecureContext){navigator.clipboard.writeText(text).then(done).catch(execCopy)}else{execCopy()}}
function rttCls(ms){return ms>100?'bad':ms>40?'warn':'ok'}
function lossCls(p){return p>2?'bad':p>.5?'warn':'ok'}
// Double-tap confirm
function routeConfirm(btnId,action){if(armTimers[btnId]){clearTimeout(armTimers[btnId]);delete armTimers[btnId];$(btnId).classList.remove('arm');action()}else{$(btnId).classList.add('arm');armTimers[btnId]=setTimeout(()=>{$(btnId).classList.remove('arm');delete armTimers[btnId]},3000)}}
// Setup
function updateMode(){const ip=($('cfgPeer').value||'').trim(),nm=($('cfgRemoteName').value||'').trim(),p=$('modePill'),l=$('modeLabel');if(ip){p.className='mpill dir';l.textContent='Direct IP'}else if(nm){p.className='mpill rdv';l.textContent='Cloud'}else{p.className='mpill pas';l.textContent='Passive'}}
function nodeNameDirty(){cfgDirty.node=true;$('nodeNameSave').classList.toggle('show',$('cfgNode').value!==savedNodeName)}
function saveNodeName(){const n=$('cfgNode').value.trim();if(!n)return;savedNodeName=n;$('nodeNameSave').classList.remove('show');$('nodeLine').textContent=n}
function togglePw(){const f=$('cfgLinkPw'),b=$('pwToggleBtn');f.type=f.type==='password'?'text':'password';b.textContent=f.type==='password'?'Show':'Hide'}
async function applySetup(){const btn=$('applyBtn');if(btn.classList.contains('done'))return;if(btn.classList.contains('arm')){clearTimeout(btn._armTimer);btn.classList.remove('arm');btn.disabled=true;btn.textContent='Rebuilding…';const body={remote:$('cfgPeer').value||'',remote_device_name:$('cfgRemoteName').value||'',link_password:$('cfgLinkPw').value||undefined,node_id:$('cfgNode').value||savedNodeName,token:$('cfgToken').value||undefined,channels:Number($('cfgChannels').value)||2,opus_bitrate_per_channel:Number($('bitrate').value)||128000,receive_buffer_ms:Number($('rxBuffer').value)||120,rendezvous_url:$('cfgRendezvous').value||undefined,phase_lock:$('cfgPhaseLock').checked,encoder_mode:$('cfgEncoderMode').value};try{const res=await fetch('/api/setup/apply',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});const txt=await res.text();if(res.ok){btn.textContent='✓ Engine rebuilding';btn.classList.add('done');btn.disabled=false;setTimeout(()=>{btn.classList.remove('done');btn.textContent='Apply & rebuild engine'},3000)}else{btn.disabled=false;btn.classList.remove('arm');btn.textContent='Apply & rebuild engine';alert('Apply failed: '+txt)}}catch(e){btn.disabled=false;btn.classList.remove('arm');btn.textContent='Apply & rebuild engine';alert('Apply failed: '+e)}}else{btn.classList.add('arm');btn.textContent='Confirm rebuild';btn._armTimer=setTimeout(()=>{btn.classList.remove('arm');btn.textContent='Apply & rebuild engine'},3000)}}
function updateSetupFromStatus(){if(!cfgDirty.node){$('cfgNode').value=status.node_id||'';if(!savedNodeName)savedNodeName=status.node_id||''}if(!cfgDirty.remoteName)$('cfgRemoteName').value=status.runtime?.remote_device_name||'';if(!cfgDirty.peer)$('cfgPeer').value=(status.runtime?.remote_host||'').split(':')[0];if(!cfgDirty.rendezvous)$('cfgRendezvous').value=status.runtime?.rendezvous_url||'https://audiolink.amsound.co.uk';if(!cfgDirty.channels)setSelectValue('cfgChannels',status.local_channels||2);if(!cfgDirty.bitrate)setSelectValue('bitrate',status.runtime?.opus_bitrate_per_channel||128000);if(!cfgDirty.rxBuffer)setSelectValue('rxBuffer',status.runtime?.latency_ms||120);if(!cfgDirty.encoderMode)setSelectValue('cfgEncoderMode',status.runtime?.encoder_mode||'music');if(!cfgDirty.phaseLock){const pl=$('cfgPhaseLock');if(pl)pl.checked=status.runtime?.phase_lock!==false}updateMode()}
// ── Signal lamp helpers ──────────────────────────────────────────────────────
// Returns dBFS for a source from current stats
function sourceDb(srcId){
  if(srcId.startsWith('input:')){const ch=parseInt(srcId.slice(6));return(stats.input_peak_dbfs||[])[ch]??-120}
  if(srcId==='ebu:l'){// EBU L: -18dBFS with 250ms silence every 3s
    const pos=(Date.now()%3000);return pos<250?-120:-18}
  if(srcId==='ebu:r')return -18;
  if(srcId.startsWith('peer:remote:ch:')){const ch=parseInt(srcId.split(':').pop());return(stats.rx_peak_dbfs||[])[ch]??-120}
  return -120}
function lampClass(db){if(db>-10)return'red';if(db>-18)return'orange';if(db>-90)return'green';return''}
// Update all rsig lamps without rebuilding the table
function updateSignalLamps(){
  document.querySelectorAll('[data-src-id]').forEach(el=>{
    const db=sourceDb(el.dataset.srcId);
    el.className='rsig '+lampClass(db)})}
// ── Home render ──────────────────────────────────────────────────────────────
function render(){
  const st=status.peer_status||'gray';
  const remote=status.remote||{};
  const badge=$('connBadge'),lamp=$('peerLamp'),pt=$('peerText');
  if(st==='green'){badge.className='conn-badge green';lamp.className='lamp green';pt.textContent='Connected — '+(remote.node_id||'remote device');$('alertbar').className='alertbar'}
  else if(st==='orange'){badge.className='conn-badge orange';lamp.className='lamp orange';pt.textContent='Remote device degraded';$('alertbar').className='alertbar degraded show';$('alertbar').textContent='REMOTE DEVICE DEGRADED'}
  else{badge.className='conn-badge gray';lamp.className='lamp gray';pt.textContent='No connected device';$('alertbar').className='alertbar offline show';$('alertbar').textContent='REMOTE DEVICE OFFLINE'}
  $('localName').textContent=status.node_id||'—';$('nodeLine').textContent=status.node_id||'—';
  $('localCh').textContent=(status.local_channels||0)+' ch';$('localBr').textContent=status.runtime?.opus_bitrate_per_channel?Math.round(status.runtime.opus_bitrate_per_channel/1000)+' kb/s':'—';
  $('localCodec').textContent=status.runtime?.encoder_mode||'music';
  const plc=$('localPl');if(status.runtime?.phase_lock!==false){plc.textContent='phase lock';plc.className='nsub-chip live'}else{plc.textContent='no phase lock';plc.className='nsub-chip'}
  const rn=$('remoteNameHome');
  if(st!=='gray'){rn.textContent=remote.node_id||status.runtime?.remote_device_name||'—';rn.style.color='var(--ink)';const chips=[];if((remote.channels||0)>0)chips.push(remote.channels+' ch');$('remoteChips').innerHTML=chips.map(ch=>`<span class="nsub-chip">${esc(ch)}</span>`).join('')}
  else{rn.textContent='—';rn.style.color='var(--muted)';$('remoteChips').innerHTML=''}
  const ow=stats.one_way_latency_ms||0;$('owVal').innerHTML=ow.toFixed(1)+'<span class="u"> ms</span>';$('owVal').className='sval '+rttCls(ow);
  $('txBw').textContent=(stats.tx_mbps||0).toFixed(3);$('rxBw').textContent=(stats.rx_mbps||0).toFixed(3);
  const loss=stats.loss_percent||0;$('lossVal').innerHTML=loss.toFixed(2)+'<span class="u"> %</span>';$('lossVal').className='sval '+lossCls(loss);
  const jit=stats.jitter_ms||0;$('jitterVal').innerHTML=jit.toFixed(1)+'<span class="u"> ms</span>';$('jitterVal').className='sval '+(jit>20?'warn':'ok');
  // Buffer bar
  const fill=stats.fill_ms||0,target=stats.target_ms||status.runtime?.latency_ms||120,MAX=target*2;
  const fp=Math.min(100,fill/MAX*100),tp=Math.min(100,target/MAX*100);
  const bf=$('bufFill');bf.style.width=fp+'%';
  bf.className='buffill'+(fill<20?' bad':fill<60?' warn':'');
  $('bufTick').style.left=tp+'%';
  // Position the legend target label at the same % as the tick
  const lbl=$('bufTickLbl');if(lbl){lbl.textContent=target+' ms';lbl.style.left=tp+'%'}
  $('fillMs').textContent=Number.isFinite(fill)?Math.round(fill):fill;$('targetMs').textContent=target;const blm=$('bufLegMax');if(blm)blm.textContent=MAX+' ms';
  $('tMode').textContent=status.runtime?.remote_host?'direct IP':(status.runtime?.remote_device_name?'cloud':'passive');$('tHost').textContent=status.runtime?.remote_host||'—';
  $('dRtt').textContent=(stats.rtt_ms||0).toFixed(1)+' ms';$('dOwLat').textContent=(stats.one_way_latency_ms||0).toFixed(1)+' ms';
  const d=stats.drift_pressure_ppm||0;$('dDrift').textContent=(d>=0?'+':'')+d+' ppm';
  $('dFps').textContent=(stats.decoded_fps||0).toFixed(1);$('dTxFps').textContent=(stats.tx_fps||0).toFixed(1);
  $('dQ').textContent=stats.queued_groups||0;$('dOv').textContent=stats.ring_overflows||0;
  $('dPlc').textContent=stats.plc_channels||0;$('dMiss').textContent=stats.seq_missing||0;
  $('dSug').textContent=stats.recommended_buffer_ms?stats.recommended_buffer_ms+' ms':'—';
  $('dUf').textContent=stats.output_underflows||0;$('dUp').textContent=(status.uptime_seconds||0)+' s';
  renderMeters();updateSetupFromStatus();updateSignalLamps()}
// ── Meters ───────────────────────────────────────────────────────────────────
const DB_FLOOR=-60,DB_RANGE=60,DB_G=-18,DB_O=-10;
function segH(db){if(!Number.isFinite(db)||db<=DB_FLOOR-.5)return{g:0,o:0,r:0};const s=Math.max(DB_FLOOR,Math.min(0,db));return{g:Math.max(0,(Math.min(s,DB_G)-DB_FLOOR)/DB_RANGE*100),o:Math.max(0,(Math.min(s,DB_O)-DB_G)/DB_RANGE*100),r:Math.max(0,(s-DB_O)/DB_RANGE*100)}}
function dbTop(db){return(1-(db-DB_FLOOR)/DB_RANGE)*100}
function makeMeter(lbl,key,ticks){const t=ticks?`<div class="barticks"><div class="tlbl hi" style="top:${dbTop(0)}%">0</div><div class="tlbl" style="top:${dbTop(DB_O)}%">-10</div><div class="tlbl" style="top:${dbTop(DB_G)}%">-18</div><div class="tlbl" style="top:${dbTop(DB_FLOOR)}%">-60</div></div>`:'';return`<div class="meter"><div class="bartrack" style="${ticks?'margin-left:32px':''}">${t}<div class="sg" id="sg_${key}" style="height:0"></div><div class="so" id="so_${key}" style="height:0;bottom:0"></div><div class="sr" id="sr_${key}" style="height:0;bottom:0"></div></div><div class="mdb" id="mdb_${key}">−∞</div><div class="mlbl">${lbl}</div></div>`}
function updateMeter(key,db){const{g,o,r}=segH(db);const G=$('sg_'+key),O=$('so_'+key),R=$('sr_'+key),D=$('mdb_'+key);if(!G)return;G.style.height=g+'%';G.style.bottom='0';O.style.height=o+'%';O.style.bottom=g+'%';R.style.height=r+'%';R.style.bottom=(g+o)+'%';if(D)D.textContent=db>-100?db.toFixed(1):'−∞'}
function renderMeters(){
  const txPeaks=stats.tx_peak_dbfs||[];const rxPeaks=stats.rx_peak_dbfs||[];const monPeaks=stats.monitor_peak_dbfs||[-120,-120];
  const connected=(status.peer_status==='green'||status.peer_status==='orange');
  const txDests=(matrix.destinations||[]).filter(d=>d.kind==='network_send');
  const rxSrcs=connected?(matrix.sources||[]).filter(s=>s.kind==='network_receive'):[];
  const txN=Math.max(txPeaks.length,txDests.length,1);const rxN=connected?Math.max(rxPeaks.length,rxSrcs.length):0;
  const txBank=$('txMeters'),rxBank=$('rxMeters'),monBank=$('monMeters');if(!txBank)return;
  const txKey='tx'+txN+(txDests.map(d=>d.label).join('|'));
  if(txBank.dataset.key!==txKey){txBank.innerHTML=txDests.map((d,i)=>{const lbl=d.label.replace(/^Send \d+ — /,'');return makeMeter('Ch '+(i+1)+(lbl?' — '+lbl:''),`tx${i}`,i===0)}).join('')||makeMeter('Ch 1','tx0',true);txBank.dataset.key=txKey}
  if(rxBank.dataset.key!==String(rxN)){rxBank.innerHTML=rxN>0?Array.from({length:rxN},(_,i)=>makeMeter('Ch '+(i+1),`rx${i}`,i===0)).join(''):'<div style="color:var(--muted);font-size:12px;padding:8px 0">No connected remote device</div>';rxBank.dataset.key=String(rxN)}
  if(!monBank.children.length){monBank.innerHTML=makeMeter('L','mn0',true)+makeMeter('R','mn1',false)}
  txPeaks.forEach((v,i)=>updateMeter('tx'+i,v));rxPeaks.forEach((v,i)=>updateMeter('rx'+i,v));[monPeaks[0]??-120,monPeaks[1]??-120].forEach((v,i)=>updateMeter('mn'+i,v))}
// ── Hover highlight ──────────────────────────────────────────────────────────
function setupHover(tableId){const tbl=$(tableId);if(!tbl)return;let pRow=null,pCols=[];function clear(){if(pRow)pRow.classList.remove('hl-row');pCols.forEach(c=>c.classList.remove('hl-col'));pRow=null;pCols=[]}tbl.addEventListener('mouseover',e=>{const cell=e.target.closest('td,th');if(!cell)return;clear();const tr=cell.parentElement;if(tr.parentElement.tagName==='TBODY'){tr.classList.add('hl-row');pRow=tr}const col=cell.cellIndex;const cols=Array.from(tbl.querySelectorAll(`tr>*:nth-child(${col+1})`));cols.forEach(c=>c.classList.add('hl-col'));pCols=cols});tbl.addEventListener('mouseleave',clear)}
// ── Label logic ──────────────────────────────────────────────────────────────
// srcUserLabels is the canonical store. Labels are attached to the physical source,
// not to the send channel. The send channel header reflects whichever source is routed there.
// On startup, pre-populate srcUserLabels from server dest labels (strip "Send N — " prefix).
function syncLabelsFromMatrix(){
  (matrix.destinations||[]).filter(d=>d.kind==='network_send').forEach(d=>{
    const serverLabel=d.label.replace(/^Send \d+ — /,'');
    // Find which source is routed to this dest
    const route=(matrix.routes||[]).find(r=>r.destination===d.id&&!r.source.startsWith('ebu:'));
    if(route&&serverLabel&&serverLabel!==`Ch ${(matrix.destinations.indexOf(d)+1)}`){
      // Server has a non-default label for this dest — attribute it to the source
      if(!srcUserLabels[route.source])srcUserLabels[route.source]=serverLabel}
  })}
// Get the user label for a source id
function getSrcLabel(srcId){const v=srcUserLabels[srcId];return v===undefined?'':v}
// Get the column header label for a send dest: derived from routed sources
function getColLabel(destId){
  const routes=(matrix.routes||[]).filter(r=>r.destination===destId&&!r.source.startsWith('ebu:'));
  if(!routes.length)return'';
  if(routes.length===1)return getSrcLabel(routes[0].source);
  // Multiple sources mixed — concatenate their labels
  const labels=routes.map(r=>getSrcLabel(r.source)).filter(Boolean);
  return labels.join(' + ')||''}
// Save ALL source labels to the server (always, even without routes)
// Labels array aligns with send channel destinations; empty string for channels with no label
async function pushAllLabels(){
  const txDests=(matrix.destinations||[]).filter(d=>d.kind==='network_send');
  const labels=txDests.map(d=>{
    const routes=(matrix.routes||[]).filter(r=>r.destination===d.id&&!r.source.startsWith('ebu:'));
    if(!routes.length)return '';  // keep existing server label
    if(routes.length===1)return getSrcLabel(routes[0].source)||d.label.replace(/^Send \d+ — /,'');
    const mixed=routes.map(r=>getSrcLabel(r.source)).filter(Boolean);
    return mixed.join(' + ')||d.label.replace(/^Send \d+ — /,'')});
  try{const res=await fetch('/api/local-labels',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({labels})});const newMatrix=await res.json();matrix=newMatrix;matrixLastKey='';buildMatrices()}catch(e){console.error(e)}}
async function saveSrcLabel(srcId,label){
  srcUserLabels[srcId]=label.trim();
  refreshTxColHeaders();
  await pushAllLabels()}
// ── Route toggle ─────────────────────────────────────────────────────────────
async function doRouteToggle(srcId,destId){
  if(routeBusy)return;routeBusy=true;
  try{
    let routes=[...(matrix.routes||[])];
    const key=srcId+'>'+destId;
    const exists=routes.findIndex(r=>r.source===srcId&&r.destination===destId);
    if(exists>=0){
      routes.splice(exists,1)  // remove existing crosspoint
    }else{
      // Adding a route: if src is physical_input, remove any tone routes to this dest first
      const isInput=srcId.startsWith('input:');
      if(isInput){routes=routes.filter(r=>!(r.destination===destId&&(r.source==='ebu:l'||r.source==='ebu:r')))}
      // If src is a tone, remove any physical_input routes to this dest first
      const isTone=srcId==='ebu:l'||srcId==='ebu:r';
      if(isTone){routes=routes.filter(r=>!(r.destination===destId&&r.source.startsWith('input:')))}
      routes.push({source:srcId,destination:destId})}
    const body={routes};
    if(destId.startsWith('output:'))body.monitor_mode='patch_matrix';
    const res=await fetch('/api/routes',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
    matrix=await res.json();matrixLastKey='';buildMatrices();
    // After route change, push labels so column headers reflect new routing
    await pushAllLabels()
  }finally{routeBusy=false}}
// ── Send routing ─────────────────────────────────────────────────────────────
function buildTxTable(){
  const txSrcsReg=(matrix.sources||[]).filter(s=>s.kind==='physical_input');
  const txSrcsUtil=(matrix.sources||[]).filter(s=>s.kind==='test_tone');
  const txDests=(matrix.destinations||[]).filter(d=>d.kind==='network_send');
  const activeRoutes=new Set((matrix.routes||[]).map(r=>r.source+'>'+r.destination));
  const numCh=txDests.length;
  if(!numCh){$('txBody').innerHTML='<tr><td style="padding:16px;color:var(--muted);font-size:12px" colspan="1">No send channels configured</td></tr>';$('txHead').innerHTML='';return}
  // Column headers
  let hRow=`<tr><th class="rh"></th>`;
  txDests.forEach((d,i)=>{const lbl=getColLabel(d.id);hRow+=`<th><div class="ch-hdr"><div class="ch-hdr-num">${i+1}</div><div class="ch-hdr-lbl" id="chlbl_${esc(d.id)}">${esc(lbl)}</div></div></th>`});
  hRow+='</tr>';$('txHead').innerHTML=hRow;
  // Body rows
  let rows='';
  function srcRow(src,editable){
    const lbl=editable?getSrcLabel(src.id):'';
    const sysName=src.kind==='physical_input'?src.label.replace(/Local Input/,'Local In'):src.label;
    // Use data-src-id on the rsig span so updateSignalLamps() can drive it
    const labelPart=editable?`<span class="src-sep">•</span><input class="src-label-input" data-src="${esc(src.id)}" value="${esc(lbl)}" maxlength="20" spellcheck="false" autocomplete="off" placeholder="label…">`:'';
    rows+=`<tr><td class="rh"><div class="src-row"><span class="rsig" data-src-id="${esc(src.id)}"></span><span class="src-sysname">${esc(sysName)}</span>${labelPart}</div></td>`;
    txDests.forEach(d=>{const on=activeRoutes.has(src.id+'>'+d.id);rows+=`<td class="xcell${on?' on':''}" data-src="${esc(src.id)}" data-dst="${esc(d.id)}"></td>`});
    rows+='</tr>'}
  txSrcsReg.forEach(s=>srcRow(s,true));
  if(txSrcsUtil.length){rows+=`<tr class="src-spacer"><td class="rh" style="height:10px"></td>${'<td style="height:10px;background:var(--bg);width:60px"></td>'.repeat(numCh)}</tr>`;txSrcsUtil.forEach(s=>srcRow(s,false))}
  $('txBody').innerHTML=rows;
  // Wire label inputs
  const inputs=Array.from(document.querySelectorAll('#txBody .src-label-input'));
  inputs.forEach((el,idx)=>{
    el.addEventListener('focus',()=>el.select());
    el.addEventListener('input',()=>{el.classList.add('dirty');srcUserLabels[el.dataset.src]=el.value;refreshTxColHeaders()});
    el.addEventListener('keydown',e=>{
      if(e.key==='Enter'){e.preventDefault();saveSrcLabel(el.dataset.src,el.value);el.classList.remove('dirty');const nx=inputs[idx+1];nx?nx.focus():el.blur()}
      if(e.key==='Escape'){el.value=getSrcLabel(el.dataset.src);el.classList.remove('dirty');el.blur()}});
    el.addEventListener('blur',()=>{if(el.classList.contains('dirty')){saveSrcLabel(el.dataset.src,el.value);el.classList.remove('dirty')}})});
  $('txBody').addEventListener('click',e=>{const cell=e.target.closest('.xcell');if(cell&&!e.target.closest('input'))doRouteToggle(cell.dataset.src,cell.dataset.dst)});
  setupHover('txTable');
  updateSignalLamps()}
function refreshTxColHeaders(){
  const txDests=(matrix.destinations||[]).filter(d=>d.kind==='network_send');
  txDests.forEach(d=>{const el=document.getElementById('chlbl_'+d.id);if(el)el.textContent=getColLabel(d.id)})}
// 1:1 and clear: preserve non-input routes
function doTx1to1(){
  const txSrcsReg=(matrix.sources||[]).filter(s=>s.kind==='physical_input');
  const txDests=(matrix.destinations||[]).filter(d=>d.kind==='network_send');
  let routes=(matrix.routes||[]).filter(r=>!r.source.startsWith('input:'));
  const n=Math.min(txSrcsReg.length,txDests.length);
  for(let i=0;i<n;i++)routes.push({source:txSrcsReg[i].id,destination:txDests[i].id});
  fetch('/api/routes',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({routes})}).then(r=>r.json()).then(async m=>{matrix=m;matrixLastKey='';buildMatrices();await pushAllLabels()}).catch(console.error)}
function doTxClear(){
  const routes=(matrix.routes||[]).filter(r=>!r.source.startsWith('input:'));
  fetch('/api/routes',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({routes})}).then(r=>r.json()).then(async m=>{matrix=m;matrixLastKey='';buildMatrices();await pushAllLabels()}).catch(console.error)}
// ── Receive routing ──────────────────────────────────────────────────────────
function doResetLabels(){Object.keys(srcUserLabels).forEach(k=>delete srcUserLabels[k]);const txDests=(matrix.destinations||[]).filter(d=>d.kind==='network_send');const labels=txDests.map(()=>'');fetch('/api/local-labels',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({labels})}).then(r=>r.json()).then(m=>{matrix=m;matrixLastKey='';buildMatrices()}).catch(console.error)}
function buildRxTable(){
  const connected=(status.peer_status==='green'||status.peer_status==='orange');
  const rxSrcs=connected?(matrix.sources||[]).filter(s=>s.kind==='network_receive'):[];
  const rxDests=(matrix.destinations||[]).filter(d=>d.kind==='physical_output');
  const activeRoutes=new Set((matrix.routes||[]).map(r=>r.source+'>'+r.destination));
  const numOut=rxDests.length;
  if(!rxSrcs.length||!numOut){
    $('rxHead').innerHTML=`<tr><th class="rh"></th></tr>`;
    $('rxBody').innerHTML='<tr><td style="padding:16px;color:var(--muted);font-size:12px" colspan="1">No connected remote device</td></tr>';return}
  let hRow=`<tr><th class="rh"></th>`;
  rxDests.forEach(d=>{const n=d.label.replace('Local Output ','Out ');hRow+=`<th><div class="ch-hdr"><div class="ch-hdr-num">${esc(n)}</div></div></th>`});
  hRow+='</tr>';$('rxHead').innerHTML=hRow;
  let rows='';
  rxSrcs.forEach(src=>{
    // Server sends label as "DeviceName • ChLabel" (• separator added in Rust)
    const sep=src.label.indexOf(' \u2022 ');
    const sysName=sep>=0?src.label.slice(0,sep):src.label;
    const chName=sep>=0?src.label.slice(sep+3):'';
    const labelPart=chName?`<span class="src-sep">\u2022</span><span style="font-size:11px;font-weight:600;color:var(--ink)">${esc(chName)}</span>`:'';
    rows+=`<tr><td class="rh"><div class="src-row"><span class="rsig" data-src-id="${esc(src.id)}"></span><span class="src-sysname">${esc(sysName)}</span>${labelPart}</div></td>`;
    rxDests.forEach(d=>{const on=activeRoutes.has(src.id+'>'+d.id);rows+=`<td class="xcell${on?' on':''}" data-src="${esc(src.id)}" data-dst="${esc(d.id)}"></td>`});
    rows+='</tr>'});
  $('rxBody').innerHTML=rows;
  $('rxBody').addEventListener('click',e=>{const cell=e.target.closest('.xcell');if(cell)doRouteToggle(cell.dataset.src,cell.dataset.dst)});
  setupHover('rxTable');
  updateSignalLamps()}
function doRx1to1(){
  const rxSrcs=(matrix.sources||[]).filter(s=>s.kind==='network_receive');
  const rxDests=(matrix.destinations||[]).filter(d=>d.kind==='physical_output');
  const nonRx=(matrix.routes||[]).filter(r=>!r.source.startsWith('peer:remote:'));
  const routes=[...nonRx];const n=Math.min(rxSrcs.length,rxDests.length);
  for(let i=0;i<n;i++)routes.push({source:rxSrcs[i].id,destination:rxDests[i].id});
  fetch('/api/routes',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({routes,monitor_mode:'patch_matrix'})}).then(r=>r.json()).then(m=>{matrix=m;matrixLastKey='';buildMatrices()}).catch(console.error)}
function doRxClear(){
  const routes=(matrix.routes||[]).filter(r=>!r.source.startsWith('peer:remote:'));
  fetch('/api/routes',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({routes})}).then(r=>r.json()).then(m=>{matrix=m;matrixLastKey='';buildMatrices()}).catch(console.error)}
// ── Matrix key + build ───────────────────────────────────────────────────────
function matrixKey(){return JSON.stringify({p:status.peer_status||'gray',s:(matrix.sources||[]).map(s=>s.id+s.kind+s.label),d:(matrix.destinations||[]).map(d=>d.id+d.label),r:(matrix.routes||[]).map(r=>r.source+r.destination).sort()})}
function buildMatrices(){
  syncLabelsFromMatrix();  // absorb any server-side label changes
  const k=matrixKey();if(k===matrixLastKey){refreshTxColHeaders();updateSignalLamps();return}
  matrixLastKey=k;buildTxTable();buildRxTable()}
// ── Audio devices ────────────────────────────────────────────────────────────
function renderDevices(){$('devices').innerHTML=`<div class="devrow"><span class="dk">Sample rate</span><b>${devices.sample_rate||48000} Hz</b></div><div class="devrow"><span class="dk">Default input</span><b>${esc(devices.default_input||'none')}</b></div><div class="devrow"><span class="dk">Input channels</span><b>${devices.default_input_channels??0}</b></div><div class="devrow"><span class="dk">Default output</span><b>${esc(devices.default_output||'none')}</b></div><div class="devrow"><span class="dk">Output channels</span><b>${devices.default_output_channels??0}</b></div>`}
// ── Offline overlay ──────────────────────────────────────────────────────────
let pollFailCount=0;
function showOffline(){document.body.innerHTML='<div style="position:fixed;inset:0;background:#080a0d;display:flex;align-items:center;justify-content:center;z-index:999"><div style="text-align:center;color:#7e8fa0;font:14px/2 system-ui,sans-serif"><div style="font-size:22px;font-weight:700;color:#e04040;margin-bottom:8px">Device offline</div></div></div>'}
// ── Poll ─────────────────────────────────────────────────────────────────────
async function poll(){
  try{
    [status,stats,matrix]=await Promise.all([fetch('/api/status').then(r=>r.json()),fetch('/api/stats').then(r=>r.json()),fetch('/api/routes').then(r=>r.json())]);
    pollFailCount=0;render();buildMatrices()
  }catch(e){
    pollFailCount++;if(pollFailCount>=3)showOffline()}}
// ── Init ─────────────────────────────────────────────────────────────────────
fetch('/api/device-ip').then(r=>r.json()).then(d=>{$('deviceIpInput').value=d.ip||'unknown'}).catch(()=>{const el=$('deviceIpInput');if(el)el.value='unknown'});
fetch('/api/audio/devices').then(r=>r.json()).then(d=>{devices=d;renderDevices()}).catch(console.error);
poll();setInterval(poll,200);
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
    // Remote device name is optional: blank = passive responder mode (no rendezvous lookup).
    // Both node_id and at least one of remote_device_name/remote must be non-empty for
    // a useful link, but the engine will start and await an initiator either way.
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
    } else if remote_device_name.is_empty() {
        // No remote device name: passive/open mode — use the dev default token.
        // This is intentional: the operator will connect any initiator that presents
        // the matching default token. For production use, set a link password or
        // configure both device names.
        (token_to_hex(&DEFAULT_SHARED_TOKEN), false)
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
    // Remote device name is optional — blank = passive responder.
    if !remote_device_name.is_empty() {
        args.insert(1, remote_device_name.to_string());
        args.insert(1, "--remote-name".to_string());
    }
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
    // phase_lock comes from the request if present; otherwise preserves the current runtime value.
    let phase_lock = req.phase_lock.unwrap_or(state.runtime.phase_lock);
    if !phase_lock { args.push("--no-phase-lock".to_string()); }
    // encoder_mode: parse the request string, fall back to current runtime value.
    let encoder_mode = req.encoder_mode.as_deref()
        .map(EncoderMode::parse)
        .unwrap_or_else(|| EncoderMode::parse(&state.runtime.encoder_mode));
    args.push("--encoder-mode".to_string());
    args.push(encoder_mode.as_str().to_string());
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
        phase_lock: Some(phase_lock),
        encoder_mode: Some(encoder_mode),
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

fn get_local_ip() -> String {
    // Connect UDP to a public address to discover which local interface the OS would use.
    // No packets are actually sent; this is purely for interface detection.
    std::net::UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| { s.connect("8.8.8.8:53")?; Ok(s) })
        .and_then(|s| s.local_addr())
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

#[derive(Debug, Serialize)]
struct DeviceIpResponse {
    ip: String,
    port: u16,
}

async fn device_ip_handler() -> Json<DeviceIpResponse> {
    Json(DeviceIpResponse { ip: get_local_ip(), port: PORT })
}

async fn remotestatus_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": 0 }))
}

async fn events_handler(ws: WebSocketUpgrade, State(state): State<WebState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| events_socket(socket, state))
}

fn control_web_state(node_id: String, shared_token: [u8; 16], send_channels: usize, opus_bitrate_per_channel: u32, jitter: JitterConfig, encoder_mode: EncoderMode) -> Result<WebState> {
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
            encoder_mode: encoder_mode.as_str().to_string(),
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
                .route("/api/device-ip", get(device_ip_handler))
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
    encoder_mode: EncoderMode,
) -> Result<()> {
    if num_channels == 0 || num_channels > MAX_CHANNELS {
        return Err(anyhow!("Channel count must be 1–{MAX_CHANNELS}"));
    }

    let is_initiator = !remote_host.trim().is_empty();
    let remote_addr_str = format!("{remote_host}:{PORT}");

    // Initiator: connect immediately. Responder: bind only and late-connect on first valid probe.
    // Bind with retry: when the engine restarts (e.g. after Apply), the old
    // socket may still be in TIME_WAIT for a brief window. Retry up to 10×500ms.
    let socket = {
        let mut last_err = None;
        let mut sock = None;
        for attempt in 0..10 {
            match UdpSocket::bind(format!("0.0.0.0:{PORT}")) {
                Ok(s) => { sock = Some(Arc::new(s)); break; }
                Err(e) => {
                    if attempt > 0 {
                        tracing::warn!("Port {PORT} busy (attempt {}/10), retrying…", attempt + 1);
                    }
                    last_err = Some(e);
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        }
        sock.ok_or_else(|| anyhow::anyhow!("Could not bind port {PORT}: {}", last_err.unwrap()))?
    };
    if is_initiator {
        socket.connect(&remote_addr_str)?;
    }

    // DSCP EF (Expedited Forwarding, 0xB8 = DSCP 46 << 2).
    // Disabled by default: on unmanaged internet/mobile paths DSCP may be ignored,
    // remarked, or policed in ways that can make loss worse. Enable explicitly
    // with AUDIOLINK_DSCP_EF=1 when testing on a QoS-aware network.
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let enable_dscp_ef = matches!(
            std::env::var("AUDIOLINK_DSCP_EF").ok().as_deref(),
            Some("1" | "true" | "TRUE" | "yes" | "YES" | "on" | "ON")
        );
        if enable_dscp_ef {
            let tos: libc::c_int = 0xb8;
            let rc = unsafe {
                libc::setsockopt(
                    socket.as_raw_fd(),
                    libc::IPPROTO_IP,
                    libc::IP_TOS,
                    &tos as *const libc::c_int as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as libc::socklen_t,
                )
            };
            if rc == 0 {
                tracing::info!("Network QoS: DSCP EF marking enabled (IP_TOS=0xb8)");
            } else {
                tracing::warn!(
                    "Network QoS: failed to enable DSCP EF: {}",
                    std::io::Error::last_os_error()
                );
            }
        } else {
            tracing::info!("Network QoS: DSCP EF marking disabled");
        }
    }

    // Tracks the established remote address in responder mode.
    // Only ever written by the recv thread once a valid handshake arrives from the real NAT port.
    let established_addr: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));

    // Rendezvous-derived candidate address for pre-connection probes.
    // Written by the rendezvous threads (registration and event listener).
    // The keepalive thread uses this for send_to punches when not yet connected.
    // Never used to connect() the socket — that is exclusively the recv thread's job
    // once it has seen the real NAT source address of an inbound packet.
    let punch_target: Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));

    // Bumped by non-recv threads when a reconnect/stale teardown should behave
    // like a fresh process start for jitter/decoder/playback state.
    let receive_reset_epoch = Arc::new(AtomicU64::new(0));

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
            encoder_mode: encoder_mode.as_str().to_string(),
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
         receive_buffer={}ms effective={}ms  adaptive_jitter={}  phase_lock={}  encoder={}",
        if is_initiator { "initiator" } else { "responder (waiting for incoming)" },
        if is_initiator { &remote_addr_str } else { "<blank>" },
        jitter.configured_delay_ms, jitter.target_delay_ms, jitter.adaptive, jitter.phase_lock,
        encoder_mode.as_str()
    );

    // M5 handshake / keepalive / staleness-watchdog thread.
    // Runs every 2130ms.
    //
    // Staleness watchdog: if connected but no valid packet for STALE_TIMEOUT_MS,
    // the remote NAT mapping has changed (firewall test, network switch, mobile
    // handoff). For responder/rendezvous mode: disconnect socket via AF_UNSPEC
    // so recv_from accepts from any source again. For direct-IP initiator mode:
    // just reset state — the socket stays connected, probes will re-handshake.
    // In both cases the rendezvous /api/connect fires within 10s and reconnects.
    {
        let socket_hs = Arc::clone(&socket);
        let connected_hs = Arc::clone(&handshake_connected);
        let established_hs = Arc::clone(&established_addr);
        let punch_target_hs = Arc::clone(&punch_target);
        let last_control_hs = Arc::clone(&last_control_ms);
        let last_audio_hs = Arc::clone(&last_audio_ms);
        let rtt_hs = Arc::clone(&rtt_us10);
        let reset_epoch_hs = Arc::clone(&receive_reset_epoch);
        let remote_channels_hs = Arc::clone(&remote_channels);
        let remote_device_name_hs = remote_device_name.to_string();
        let remote_addr_watchdog = remote_addr_str.clone();
        const STALE_TIMEOUT_MS: u64 = 15_000;
        std::thread::spawn(move || {
            let probe = build_probe_packet(&device_name_token, &shared_token);
            loop {
                let connected = connected_hs.load(Ordering::Relaxed);

                // ── Staleness watchdog ────────────────────────────────────────
                if connected {
                    let now = now_millis();
                    let last_any = last_control_hs.load(Ordering::Relaxed)
                        .max(last_audio_hs.load(Ordering::Relaxed));
                    if last_any > 0 && now.saturating_sub(last_any) > STALE_TIMEOUT_MS {
                        tracing::warn!(
                            "No packet for {}ms — stale connection, resetting for re-handshake",
                            now.saturating_sub(last_any)
                        );

                        if !is_initiator {
                            // Responder/rendezvous: remove the kernel peer filter so
                            // recv_from accepts packets from the new NAT address.
                            match udp_disconnect_socket(socket_hs.as_ref()) {
                                Ok(()) => tracing::info!(
                                    "UDP socket disconnected with AF_UNSPEC after stale timeout"
                                ),
                                Err(e) => tracing::warn!(
                                    "UDP AF_UNSPEC disconnect failed after stale timeout: {e}"
                                ),
                            }
                            if let Ok(mut e) = established_hs.lock() { *e = None; }
                        }
                        // Initiator: socket stays connected to original remote address.
                        // Just reset state — probes will trigger a fresh handshake.

                        connected_hs.store(false, Ordering::Relaxed);
                        last_control_hs.store(0, Ordering::Relaxed);
                        last_audio_hs.store(0, Ordering::Relaxed);
                        rtt_hs.store(0, Ordering::Relaxed);
                        // NOTE: remote_channels is NOT zeroed here. reset_receive_session! in
                        // the recv thread sets started_recv=false, which makes the output
                        // callback output silence without touching any rings. Zeroing
                        // remote_channels here causes a priming race: the recv thread primes
                        // with active=1 (0 clamped), fires started_recv=true on ring[0] alone,
                        // then metadata arrives and active jumps to N, leaving rings 1..N-1
                        // empty — output underflows until they catch up.
                        reset_epoch_hs.fetch_add(1, Ordering::Relaxed);

                        std::thread::sleep(std::time::Duration::from_millis(2130));
                        continue;
                    }
                }

                // ── Probe / keepalive ─────────────────────────────────────────
                let connected = connected_hs.load(Ordering::Relaxed);
                if is_initiator {
                    socket_hs.send(&probe).ok();
                    if connected {
                        tracing::trace!("09 07 keepalive sent");
                        // Zero RTT *before* sending the ping so a fast pong on a LAN
                        // cannot be immediately overwritten by the store that follows.
                        rtt_hs.store(0, Ordering::Relaxed);
                        let ts = now_us();
                        socket_hs.send(&build_rtt_ping(ts)).ok();
                    } else {
                        tracing::trace!("09 07 probe sent");
                    }
                } else if connected {
                    socket_hs.send(&probe).ok();
                    rtt_hs.store(0, Ordering::Relaxed);
                    let ts = now_us();
                    socket_hs.send(&build_rtt_ping(ts)).ok();
                    tracing::trace!("09 07 keepalive sent (responder)");
                } else {
                    let target = punch_target_hs.lock().ok().and_then(|g| *g);
                    match target {
                        Some(addr) => {
                            match socket_hs.send_to(&probe, addr) {
                                Ok(_) => tracing::trace!("09 07 rendezvous punch → {addr}"),
                                Err(e) => tracing::warn!("09 07 rendezvous punch to {addr} failed: {e}"),
                            }
                        }
                        None => {
                            tracing::trace!("Responder: waiting for {remote_device_name_hs}");
                        }
                    }
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
        // After the first successful registration, if we have a remote device name
        // and no direct host (pure rendezvous mode), call /api/connect to notify
        // the remote and trigger simultaneous probe firing on both sides.
        let reg_name = node_id.clone();
        let reg_remote = remote_device_name.to_string();
        let reg_base = rdv_base.clone();
        let reg_direct = is_initiator;
        let reg_connected = Arc::clone(&handshake_connected);
        let reg_socket = Arc::clone(&socket);
        let reg_punch_target = Arc::clone(&punch_target);
        let reg_established_addr = Arc::clone(&established_addr);
        let reg_probe_token = device_name_token;
        let reg_shared_token = shared_token;
        std::thread::spawn(move || {
            let reg_body = serde_json::json!({ "name": reg_name, "port": PORT }).to_string();
            let con_body = serde_json::json!({ "my_name": reg_name, "remote_name": reg_remote }).to_string();
            let mut cycle = 0u32;
            std::thread::sleep(Duration::from_millis(1500));
            loop {
                match ureq::post(&format!("{reg_base}/api/register"))
                    .set("Content-Type", "application/json")
                    .send_string(&reg_body)
                {
                    Ok(_) => {
                        tracing::debug!("rendezvous: registered {reg_name}");
                        // Call /api/connect on every 10s register cycle while not yet
                        // handshaked. Frequent retries are desirable on mobile paths where
                        // the remote may have re-registered with a new NAT address.
                        if !reg_direct && !reg_remote.is_empty()
                            && !reg_connected.load(Ordering::Relaxed)
                        {
                            match ureq::post(&format!("{reg_base}/api/connect"))
                                .set("Content-Type", "application/json")
                                .send_string(&con_body)
                            {
                                Ok(resp) => {
                                    if let Ok(text) = resp.into_string() {
                                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                            let addr_str = v["remote_addr"].as_str().unwrap_or("?");
                                            let notified = v["notified"].as_bool().unwrap_or(false);
                                            tracing::info!(
                                                "rendezvous: connect → {reg_remote} @ {addr_str} notified={notified}"
                                            );
                                            // Both sides must punch simultaneously.
                                            // The remote was notified and will fire at us;
                                            // we must also fire at the remote immediately
                                            // to open our own NAT hole before their probe arrives.
                                            if let Ok(addr) = addr_str.parse::<std::net::SocketAddr>() {
                                                // Store as the candidate punch address.
                                                // Do NOT call socket.connect() here — connecting to the
                                                // rendezvous-registered port (always 20102) would set a
                                                // kernel-level receive filter that silently drops any probe
                                                // arriving from the remote's actual NAT-mapped port, which
                                                // is almost certainly different. The recv thread will
                                                // connect() once it sees the real source address.
                                                if let Ok(mut t) = reg_punch_target.lock() {
                                                    *t = Some(addr);
                                                }
                                                // If a previous connected UDP peer filter is still installed
                                                // after ECONNREFUSED/stale teardown, send_to() may fail or be
                                                // ignored. Ensure rendezvous punches use an unconnected socket.
                                                if !reg_connected.load(Ordering::Relaxed) {
                                                    if let Ok(mut e) = reg_established_addr.lock() {
                                                        if e.is_some() {
                                                            match udp_disconnect_socket(reg_socket.as_ref()) {
                                                                Ok(()) => tracing::info!(
                                                                    "UDP socket disconnected with AF_UNSPEC before rendezvous punch"
                                                                ),
                                                                Err(err) => tracing::warn!(
                                                                    "UDP AF_UNSPEC disconnect before rendezvous punch failed: {err}"
                                                                ),
                                                            }
                                                            *e = None;
                                                        }
                                                    }
                                                }
                                                let probe = build_probe_packet(&reg_probe_token, &reg_shared_token);
                                                match reg_socket.send_to(&probe, addr) {
                                                    Ok(_) => tracing::info!("rendezvous: outbound punch at {addr}"),
                                                    Err(e) => tracing::warn!("rendezvous: outbound punch to {addr} failed: {e}"),
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(e) => tracing::warn!("rendezvous: connect request failed: {e}"),
                            }
                        }
                    }
                    Err(e) => tracing::warn!("rendezvous: register failed: {e}"),
                }
                cycle += 1;
                std::thread::sleep(Duration::from_secs(10));
            }
        });

        // Long-poll event listener — GET /api/events/{name}.
        // When a connect event arrives, late-connect the socket and fire a probe
        // to begin NAT hole punching simultaneously with the remote device.
        let event_name = node_id.clone();
        let event_socket = Arc::clone(&socket);
        let event_punch_target = Arc::clone(&punch_target);
        let event_established_addr = Arc::clone(&established_addr);
        let event_connected = Arc::clone(&handshake_connected);
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
                                    // Same reasoning as the registration thread: do NOT connect() here.
                                    // Store the rendezvous address as the punch target and use send_to
                                    // so the socket stays unconnected and recv_from can accept packets
                                    // from the remote's real NAT port.
                                    if let Ok(mut t) = event_punch_target.lock() {
                                        *t = Some(addr);
                                    }
                                    // Same as the registration path: while disconnected, make
                                    // sure an old connected-UDP peer filter cannot block the
                                    // new rendezvous punch/re-handshake.
                                    if !event_connected.load(Ordering::Relaxed) {
                                        if let Ok(mut e) = event_established_addr.lock() {
                                            if e.is_some() {
                                                match udp_disconnect_socket(event_socket.as_ref()) {
                                                    Ok(()) => tracing::info!(
                                                        "UDP socket disconnected with AF_UNSPEC before inbound-event punch"
                                                    ),
                                                    Err(err) => tracing::warn!(
                                                        "UDP AF_UNSPEC disconnect before inbound-event punch failed: {err}"
                                                    ),
                                                }
                                                *e = None;
                                            }
                                        }
                                    }
                                    let probe = build_probe_packet(&event_probe_token, &event_shared_token);
                                    match event_socket.send_to(&probe, addr) {
                                        Ok(_) => tracing::info!("rendezvous: outbound punch at {addr}"),
                                        Err(e) => tracing::warn!("rendezvous: outbound punch to {addr} failed: {e}"),
                                    }
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
        // When a session resets, the output callback drains old playback samples
        // under silence before the recv thread is allowed to prime the new stream.
        let flush_playback_samples = Arc::new(AtomicUsize::new(0));
        let flush_playback_samples_recv = Arc::clone(&flush_playback_samples);
        let flush_playback_samples_play = Arc::clone(&flush_playback_samples);
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
        let punch_target_recv = Arc::clone(&punch_target);
        let receive_reset_epoch_recv = Arc::clone(&receive_reset_epoch);
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
                let mut param: libc::sched_param = std::mem::zeroed();
                param.sched_priority = 20;
        
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

            let mut decoders = fresh_opus_decoders();

            // One Rubato FastFixedIn resampler per channel.
            // Ratio 1.0 at init; updated each loop iteration from correction_ratio.
            // PolynomialDegree::Linear gives transparent interpolation at sub-500ppm
            // rates with negligible CPU cost. The max_relative of 1.01 covers our
            // ±5000ppm control clamp with headroom.
            let mut resamplers = fresh_asrc_resamplers();

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
            // Rolling window of raw inter-arrival jitter deltas for p95 computation.
            // 300 samples ≈ 60s at 50fps. Sorted at the 5s stats interval — cost is trivial.
            // Kept separate from the EMA because the EMA aggressively smooths bursts that
            // a real buffer needs to absorb.
            const JITTER_WINDOW_SIZE: usize = 300;
            let mut jitter_window: std::collections::VecDeque<f64> = std::collections::VecDeque::with_capacity(JITTER_WINDOW_SIZE + 1);
            let mut output_underflows_at_last_stats: usize = 0;
            let mut plc_count_at_last_stats: usize = 0;
            let mut lost_packets_at_last_stats: usize = 0;
            let mut ring_overflows_at_last_stats: usize = 0;
            // Non-phase-lock decoded_fps tracking: count one group per unique RTP timestamp.
            let mut last_decoded_ts_nonpl: Option<u32> = None;
            let mut correction_ratio: f64 = 1.0; // updated each iteration; 1.0 on first drain is correct
            // PI controller state. Integral sampled at 5Hz (every 200ms) using actual elapsed
            // time — independent of recv loop rate. Eliminates steady-state fill error from a
            // constant clock offset (P-only equilibrium: fill = target + offset_ppm / K_p).
            let mut integral_correction: f64 = 0.0;
            let mut integral_last = std::time::Instant::now();
            // Overflow cooldown — log and reset integral at most once per 10 seconds.
            let mut overflow_last_warn = std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(30))
                .unwrap_or_else(std::time::Instant::now);
            // High-watermark for phase-lock drain — discard late packets for timestamps
            // that have already been PLC-processed to prevent double-decode (decoded_fps > 50).
            let mut last_drained_ts: Option<u32> = None;
            let mut observed_reset_epoch = receive_reset_epoch_recv.load(Ordering::Relaxed);
            if jitter.phase_lock {
                tracing::info!("M6 jitter: phase-locked timestamp buffer active; Rubato ASRC active");
            }

            macro_rules! reset_receive_session {
                ($reason:expr, $clear_metadata:expr) => {{
                    let active_for_flush = remote_channels_recv.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
                    let old_fill_samples = pb_prods[..active_for_flush]
                        .iter()
                        .map(|p| p.occupied_len())
                        .max()
                        .unwrap_or(0);

                    groups.clear();
                    newest_timestamp = None;
                    last_drained_ts = None;
                    last_seq.fill(None);
                    last_jitter_timestamp = None;
                    last_jitter_arrival = None;
                    jitter_ms_estimate = 0.0;
                    jitter_window.clear();
                    last_decoded_ts_nonpl = None;
                    integral_correction = 0.0;
                    correction_ratio = 1.0;
                    integral_last = std::time::Instant::now();
                    decoders = fresh_opus_decoders();
                    resamplers = fresh_asrc_resamplers();

                    media_packets_total = 0;
                    media_packets_at_last_stats = 0;
                    rx_bytes_total = 0;
                    rx_bytes_at_last_stats = 0;
                    lost_packets_total = 0;
                    lost_packets_at_last_stats = 0;
                    plc_count_total = 0;
                    plc_count_at_last_stats = 0;
                    decoded_groups_total = 0;
                    decoded_groups_at_last_stats = 0;
                    output_underflows_at_last_stats = output_underflows_recv.load(Ordering::Relaxed);
                    last_stats = Instant::now();

                    if $clear_metadata {
                        last_remote_metadata = None;
                    }

                    started_recv.store(false, Ordering::Relaxed);
                    empty_cb_recv.store(0, Ordering::Relaxed);

                    if old_fill_samples > 0 {
                        // Drain stale samples under silence before accepting new media.
                        // This makes a reconnect start from an empty playback ring, like
                        // a cold process start, instead of priming on old-session audio.
                        flush_playback_samples_recv.store(
                            old_fill_samples.saturating_add(FRAME_SAMPLES),
                            Ordering::Relaxed,
                        );
                    }

                    tracing::info!(
                        "Receive session reset ({}) — flushing {}ms of old playback state",
                        $reason,
                        old_fill_samples * 1000 / SAMPLE_RATE as usize
                    );
                }};
            }

            loop {
                let reset_epoch_now = receive_reset_epoch_recv.load(Ordering::Relaxed);
                if reset_epoch_now != observed_reset_epoch {
                    observed_reset_epoch = reset_epoch_now;
                    reset_receive_session!("watchdog/socket reset", true);
                }
                // In responder mode we use recv_from so we can extract the sender's address
                // for the late-connect. In initiator mode we use recv (socket is already connected).
                let recv_result = if is_initiator {
                    socket_recv.recv(&mut buf).map(|n| (n, None))
                } else {
                    socket_recv.recv_from(&mut buf).map(|(n, addr)| (n, Some(addr)))
                };

                match recv_result {
                    Ok((n, src_addr_opt)) => {
                        // Responder: reject packets from a different source once established,
                        // but do not late-connect yet. The first packet may be stray UDP or a
                        // stale/mismatched handshake; connecting here would install the kernel
                        // peer filter too early and silently drop the real NAT-mapped peer.
                        if !is_initiator {
                            if let Some(src_addr) = src_addr_opt {
                                let established = established_addr_recv.lock().ok().and_then(|g| *g);
                                if let Some(known) = established {
                                    if known != src_addr {
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
                                }
                            }
                        }

                        let responder_late_connect = |src_addr_opt: Option<std::net::SocketAddr>| -> bool {
                            if is_initiator {
                                return true;
                            }
                            let Some(src_addr) = src_addr_opt else {
                                tracing::warn!("Responder: validated handshake had no source address");
                                return false;
                            };

                            let mut established = match established_addr_recv.lock() {
                                Ok(guard) => guard,
                                Err(_) => {
                                    tracing::error!("Responder: established_addr lock poisoned");
                                    return false;
                                }
                            };

                            match *established {
                                Some(known) if known == src_addr => true,
                                Some(known) => {
                                    tracing::warn!(
                                        "Connection conflict: validated packet from {src_addr} but already connected to {known}"
                                    );
                                    if let Ok(mut c) = remote_conflict_recv.lock() {
                                        if c.is_none() {
                                            *c = Some(src_addr.to_string());
                                        }
                                    }
                                    false
                                }
                                None => {
                                    // First *valid* handshake from the peer. This is the real
                                    // NAT-translated source address, which may differ from the
                                    // rendezvous-registered port. Connect only after token validation
                                    // so the kernel peer filter cannot latch onto stray/stale UDP.
                                    if let Err(e) = socket_recv.connect(src_addr) {
                                        tracing::error!("Responder: late-connect to {src_addr} failed: {e}");
                                        false
                                    } else {
                                        tracing::info!("Responder: late-connected to {src_addr}");
                                        *established = Some(src_addr);
                                        if let Ok(mut t) = punch_target_recv.lock() { *t = None; }
                                        true
                                    }
                                }
                            }
                        };

                        if let Some(pkt) = parse_handshake_packet(&buf[..n]) {
                            match pkt {
                                HandshakePacket::Probe { sender_token, expected_peer } if expected_peer == shared_token => {
                                    if !responder_late_connect(src_addr_opt) { continue; }
                                    last_control_recv.store(now_millis(), Ordering::Relaxed);
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
                                        reset_receive_session!("new authenticated handshake", true);
                                        tracing::info!(
                                            "Handshake: received 09 07 from {sender_name}, sent 09 09 / 09 08 / 09 0a"
                                        );
                                        // Clear any stale conflict once a fresh valid handshake completes.
                                        if let Ok(mut c) = remote_conflict_recv.lock() { *c = None; }
                                    }
                                }
                                HandshakePacket::Accept { token } if token == shared_token => {
                                    if !responder_late_connect(src_addr_opt) { continue; }
                                    last_control_recv.store(now_millis(), Ordering::Relaxed);
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
                                        reset_receive_session!("new authenticated handshake", true);
                                        tracing::info!("Handshake: received 09 09, sent 09 08 / 09 0a");
                                        if let Ok(mut c) = remote_conflict_recv.lock() { *c = None; }
                                    }
                                }
                                HandshakePacket::Confirm { token } if token == shared_token => {
                                    if !responder_late_connect(src_addr_opt) { continue; }
                                    last_control_recv.store(now_millis(), Ordering::Relaxed);
                                    if !connected_recv.swap(true, Ordering::Relaxed) {
                                        reset_receive_session!("new authenticated handshake", true);
                                        tracing::info!("Handshake: received 09 08 confirmation");
                                        if let Ok(mut c) = remote_conflict_recv.lock() { *c = None; }
                                    }
                                }
                                HandshakePacket::Metadata(metadata) if metadata.channels > 0 => {
                                    if !connected_recv.load(Ordering::Relaxed) { continue; }
                                    last_control_recv.store(now_millis(), Ordering::Relaxed);
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
                                        reset_receive_session!("remote channel layout changed", false);
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
                                    if !connected_recv.load(Ordering::Relaxed) { continue; }
                                    last_control_recv.store(now_millis(), Ordering::Relaxed);
                                    // Echo back immediately — do not alter the timestamp.
                                    socket_recv.send(&build_rtt_pong(timestamp_us)).ok();
                                }
                                HandshakePacket::RttPong { timestamp_us } => {
                                    if !connected_recv.load(Ordering::Relaxed) { continue; }
                                    last_control_recv.store(now_millis(), Ordering::Relaxed);
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

                        if !is_initiator && !connected_recv.load(Ordering::Relaxed) {
                            // Media packets are not token-authenticated, so they must not be
                            // allowed to establish the responder socket before the handshake.
                            continue;
                        }

                        let pkt = match parse_packet(&buf[..n]) {
                            Some(p) => {
                                last_audio_recv.store(now_millis(), Ordering::Relaxed);
                                p
                            },
                            None => continue,
                        };

                        if flush_playback_samples_recv.load(Ordering::Relaxed) > 0 {
                            // We are still draining stale audio from the previous session.
                            // Drop media rather than mixing old and new sessions in the ring.
                            continue;
                        }

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
                                    // P95 window: cap at JITTER_WINDOW_SIZE samples.
                                    if jitter_window.len() >= JITTER_WINDOW_SIZE {
                                        jitter_window.pop_front();
                                    }
                                    jitter_window.push_back(delta);
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

                        if jitter.phase_lock {
                            if let Some(hwm) = last_drained_ts {
                                let rollback = hwm.wrapping_sub(pkt.timestamp);
                                let at_or_behind_hwm = rtp_timestamp_at_or_before(pkt.timestamp, hwm);
                                let seq_reset = last_seq[ch]
                                    .map(|prev| rtp_seq_looks_reset(prev, pkt.seq))
                                    .unwrap_or(false);
                                let fresh_sender_ts = pkt.timestamp <= RTP_RESTART_LOW_TS_SAMPLES;

                                if at_or_behind_hwm
                                    && rollback > RTP_RESTART_ROLLBACK_SAMPLES
                                    && (seq_reset || fresh_sender_ts)
                                {
                                    tracing::warn!(
                                        "RTP stream restart detected on ch{ch}: ts={} is {} samples behind drained ts {}, seq={}; resetting receive jitter state",
                                        pkt.timestamp, rollback, hwm, pkt.seq
                                    );
                                    reset_receive_session!("RTP timestamp/sequence restart", false);
                                }
                            }
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
                            // Discard packets whose timestamp has already been processed
                            // (PLC was generated after phase-lock timeout). Without this,
                            // the late real packet would create a new BTreeMap entry for
                            // the same timestamp, get decoded again, and push fill above
                            // the real-time rate — decoded_fps exceeds 50 and fill drifts up.
                            if let Some(hwm) = last_drained_ts {
                                if rtp_timestamp_at_or_before(pkt.timestamp, hwm) {
                                    continue;
                                }
                            }
                            let group = groups
                                .entry(pkt.timestamp)
                                .or_insert_with(|| FrameGroup::new(pkt.timestamp, active));
                            group.insert(ch, pkt.opus_payload);
                        } else {
                            // Non-phase-lock: decode immediately per channel.
                            // Count one decoded group per unique RTP timestamp so decoded_fps
                            // matches the phase-lock path (one entry per 20ms frame, not per channel).
                            if last_decoded_ts_nonpl != Some(pkt.timestamp) {
                                decoded_groups_total += 1;
                                last_decoded_ts_nonpl = Some(pkt.timestamp);
                            }
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
                        // keepalive/rendezvous paths resume probing, and in responder mode
                        // remove the connected-UDP peer filter so a new NAT port can reach us.
                        if connected_recv.swap(false, Ordering::Relaxed) {
                            tracing::info!("recv: remote disconnected (ECONNREFUSED) — waiting for reconnect");
                        }
                        if !is_initiator {
                            match udp_disconnect_socket(socket_recv.as_ref()) {
                                Ok(()) => tracing::info!(
                                    "UDP socket disconnected with AF_UNSPEC after ECONNREFUSED"
                                ),
                                Err(err) => tracing::warn!(
                                    "UDP AF_UNSPEC disconnect after ECONNREFUSED failed: {err}"
                                ),
                            }
                            if let Ok(mut e) = established_addr_recv.lock() { *e = None; }
                        }
                        // Only do the full session reset (decoder flush, ring drain, re-prime)
                        // if playback was actually running. If nothing was playing there is no
                        // state to flush and firing reset_receive_session! every 100ms produces
                        // noisy "flushing 0ms" log spam with no benefit.
                        if started_recv.load(Ordering::Relaxed) {
                            reset_receive_session!("ECONNREFUSED", true);
                        }
                        last_control_recv.store(0, Ordering::Relaxed);
                        last_audio_recv.store(0, Ordering::Relaxed);
                        rtt_us10_recv.store(0, Ordering::Relaxed);
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    Err(e) => {
                        tracing::error!("recv: {e}");
                        continue;
                    }
                }

                let active = remote_channels_recv.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);

                if jitter.phase_lock {
                    let (out, plc, drained_ts) = drain_phase_locked_groups(
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
                    if let Some(ts) = drained_ts { last_drained_ts = Some(ts); }
                }

                let fill_ms = ring_fill_ms(&pb_prods, active);
                let target_fill_ms = jitter.target_delay_ms;

                let fill_error_ms = fill_ms as f64 - target_fill_ms as f64;

                // PI controller for ASRC clock-drift correction.
                // P term responds immediately. I term accumulates every 200ms and eliminates
                // steady-state fill error from constant clock offset between sender/receiver.
                // Anti-windup clamped at ±12000ppm.
                if jitter.adaptive {
                    let elapsed_s = integral_last.elapsed().as_secs_f64();
                    if elapsed_s >= 0.2 {
                        integral_correction += fill_error_ms * 0.000003 * elapsed_s;
                        integral_correction = integral_correction.clamp(-0.012, 0.012);
                        integral_last = std::time::Instant::now();
                    }
                } else {
                    integral_correction = 0.0;
                    integral_last = std::time::Instant::now();
                }

                correction_ratio = if jitter.adaptive {
                    (1.0 - fill_error_ms * 0.000010 - integral_correction).clamp(0.980, 1.020)
                } else {
                    1.0
                };

                // Overflow backstop at 3× target. IMPORTANT: do NOT use started_recv here.
                // For underruns, started=false → silence → ring refills naturally.
                // For overflow, silence → ring never drains → immediate re-prime loop.
                // Instead: keep audio playing (ring drains at hardware rate), reset integral,
                // log at most once per 10 seconds.
                if fill_ms > jitter.target_delay_ms as usize * 3 {
                    if overflow_last_warn.elapsed() > std::time::Duration::from_secs(10) {
                        tracing::warn!(
                            "Buffer high: fill={}ms > 3× target ({}ms) — resetting integral",
                            fill_ms, jitter.target_delay_ms
                        );
                        integral_correction = 0.0;
                        integral_last = std::time::Instant::now();
                        overflow_last_warn = std::time::Instant::now();
                    }
                }

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
                    let overflows_now = ring_overflows_recv.load(Ordering::Relaxed);
                    let overflows_delta = overflows_now.saturating_sub(ring_overflows_at_last_stats);
                    ring_overflows_at_last_stats = overflows_now;

                    // P95 jitter over the rolling window. Need ≥10 samples for a
                    // meaningful percentile; fall back to EMA until then.
                    let jitter_p95_ms = if jitter_window.len() >= 10 {
                        let mut sorted: Vec<f64> = jitter_window.iter().copied().collect();
                        sorted.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        let idx = ((sorted.len() as f64 * 0.95) as usize).min(sorted.len() - 1);
                        sorted[idx]
                    } else {
                        jitter_ms_estimate
                    };
                    // Rule-of-thumb: 4× p95 absorbs the vast majority of bursts.
                    // Floor is 80ms — empirically 40ms is insufficient on WiFi paths even
                    // when p95 looks low, because WiFi has a heavy tail (power-save wakeups,
                    // channel scans) that doesn't appear in p95 of a 300-sample window.
                    let recommended_buffer_ms = ((jitter_p95_ms * 4.0).ceil() as u32)
                        .clamp(80, 500);

                    tracing::info!(
                        "RX stats: channels={active} rx={:.3}Mbps loss={:.3}% jitter_ema={:.2}ms jitter_p95={:.2}ms \
                         fill={}ms target={}ms recommend={}ms \
                         phase_lock={} queued_groups={} decoded_fps={:.1} output_underflows={} plc_channels={} \
                         seq_missing={} drift_pressure={}ppm rtt={:.1}ms one_way={:.1}ms ring_overflows={}",
                        rx_mbps, loss_percent, jitter_ms_estimate, jitter_p95_ms,
                        fill_ms, target_fill_ms, recommended_buffer_ms,
                        jitter.phase_lock, groups.len(), rx_fps, output_underflows_delta,
                        plc_delta, seq_missing_delta, ppm, rtt_ms, one_way_ms, overflows_delta
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
                        stats.jitter_p95_ms = jitter_p95_ms;
                        stats.recommended_buffer_ms = recommended_buffer_ms;
                        stats.latency_ms = fill_ms;
                        stats.rx_mbps = rx_mbps;
                        stats.drift_pressure_ppm = ppm;
                        stats.rtt_ms = rtt_ms;
                        stats.one_way_latency_ms = one_way_ms;
                        stats.ring_overflows = overflows_delta;
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
                let active_channels = remote_channels_play.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);

                if !connected_play.load(Ordering::Relaxed) || !started_play.load(Ordering::Relaxed) {
                    let mut remaining_flush = flush_playback_samples_play.load(Ordering::Relaxed);
                    if remaining_flush > 0 {
                        for frame in data.chunks_exact_mut(2) {
                            for ch in 0..active_channels {
                                let _ = pb_conss[ch].try_pop();
                            }
                            frame[0] = 0.0;
                            frame[1] = 0.0;
                            remaining_flush = remaining_flush.saturating_sub(1);
                            if remaining_flush == 0 {
                                // Keep output silent for the rest of this callback, but stop
                                // draining so the recv thread can build a fresh prime buffer.
                                break;
                            }
                        }
                        flush_playback_samples_play.store(remaining_flush, Ordering::Relaxed);
                    }
                    data.fill(0.0);
                    return;
                }
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
                (0..num_channels).map(|_| make_encoder_for_mode(opus_bitrate_per_channel, encoder_mode).unwrap()).collect();
            tracing::info!(
                "Send thread: {num_channels} Opus encoder(s) initialised — mode={} bitrate={}kb/s{}",
                encoder_mode.as_str(),
                opus_bitrate_per_channel / 1000,
                if matches!(encoder_mode, EncoderMode::Speech) { " (inband FEC on)" } else { "" },
            );
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
            let mut param: libc::sched_param = std::mem::zeroed();
            param.sched_priority = 20;
    
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

            let encoder_mode = {
                let idx = args.iter().position(|a| a == "--encoder-mode");
                idx.and_then(|i| args.get(i + 1))
                    .map(|s| EncoderMode::parse(s))
                    .unwrap_or_default()
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
                encoder_mode,
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
            eprintln!("  audiolinkd [--web ADDR:PORT] [--id NAME] [--token TOKEN] [--channels N] [--bitrate BPS] [--latency-ms MS] [--encoder-mode music|speech]");
            eprintln!("  audiolinkd bidir <REMOTE_IP> [--channels N] [--token TOKEN] [--id NAME] [--bitrate BPS] [--latency-ms MS] [--fixed-jitter] [--no-phase-lock] [--encoder-mode music|speech] [--web ADDR:PORT] [--no-web]");
            eprintln!();
            eprintln!("  --encoder-mode music   Opus Application::Audio, no FEC (default, best for programme)");
            eprintln!("  --encoder-mode speech  Opus Application::Voip, inband FEC on (halves perceived loss on mobile)");
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

            let encoder_mode = {
                let idx = args.iter().position(|a| a == "--encoder-mode");
                idx.and_then(|i| args.get(i + 1))
                    .map(|s| EncoderMode::parse(s))
                    .unwrap_or_else(|| persisted.encoder_mode.unwrap_or_default())
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
                    encoder_mode,
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
                    encoder_mode,
                )
            } else {
                let state = control_web_state(node_id.clone(), shared_token, num_channels, opus_bitrate_per_channel, jitter, encoder_mode)?;
                tracing::info!("AudioLink Web UI starting — open http://{web_addr} and use Setup to connect");
                spawn_web_ui(web_addr, state);
                loop { std::thread::sleep(Duration::from_secs(3600)); }
            }
        }
    }
}
