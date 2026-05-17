use std::convert::TryInto;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::Result;
use serde::Serialize;
use crate::constants::*;

// ─── Utility ─────────────────────────────────────────────────────────────────

pub fn now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn json_escape(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"'  => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

pub fn local_channel_label(ch: usize) -> String {
    format!("Ch {}", ch + 1)
}

pub fn default_node_id() -> String {
    if let Ok(v) = std::env::var("HOSTNAME") {
        let v = v.trim();
        if !v.is_empty() { return v.to_string(); }
    }
    if let Ok(out) = Command::new("hostname").output() {
        if out.status.success() {
            let v = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if !v.is_empty() { return v; }
        }
    }
    "unknown-host".to_string()
}

// ─── RTP header ──────────────────────────────────────────────────────────────

pub fn rtp_header(ssrc: u32) -> Vec<u8> {
    let mut pkt = Vec::with_capacity(64);
    pkt.push(0x80);
    pkt.push(0x69);
    pkt.extend_from_slice(&0u16.to_be_bytes());
    pkt.extend_from_slice(&0u32.to_be_bytes());
    pkt.extend_from_slice(&ssrc.to_be_bytes());
    pkt
}

pub fn be_u32_at(data: &[u8], offset: usize) -> Option<u32> {
    Some(u32::from_be_bytes(data.get(offset..offset + 4)?.try_into().ok()?))
}

pub fn copy_token(data: &[u8], offset: usize) -> Option<[u8; 16]> {
    Some(data.get(offset..offset + 16)?.try_into().ok()?)
}

// ─── Media packets ────────────────────────────────────────────────────────────

pub fn build_packet(seq: u16, timestamp: u32, channel: u8, opus: &[u8]) -> Vec<u8> {
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
pub struct MediaPacket<'a> {
    pub seq: u16,
    pub timestamp: u32,
    pub channel: u8,
    pub opus_payload: &'a [u8],
}

pub fn parse_packet(data: &[u8]) -> Option<MediaPacket<'_>> {
    if data.len() < 19 { return None; }
    if data[0] != 0x80 || data[1] != 0x69 { return None; }
    if be_u32_at(data, 8)? != SSRC_MEDIA { return None; }
    if data[12] != 0x09 || data[13] != 0x06 { return None; }
    Some(MediaPacket {
        seq: u16::from_be_bytes(data.get(2..4)?.try_into().ok()?),
        timestamp: be_u32_at(data, 4)?,
        channel: data[18],
        opus_payload: &data[19..],
    })
}

// ─── Control packets ──────────────────────────────────────────────────────────

pub fn build_probe_packet(device_name_token: &[u8; 16], shared_token: &[u8; 16]) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x07]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(device_name_token);
    pkt.extend_from_slice(shared_token);
    pkt
}

pub fn build_accept_packet(shared_token: &[u8; 16]) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x09]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(shared_token);
    pkt
}

pub fn build_confirm_packet(shared_token: &[u8; 16]) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x08]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(shared_token);
    pkt.extend_from_slice(shared_token);
    pkt
}

pub fn build_rtt_ping(timestamp_us: u64) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x0b]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(&timestamp_us.to_be_bytes());
    pkt
}

pub fn build_rtt_pong(timestamp_us: u64) -> Vec<u8> {
    let mut pkt = rtp_header(SSRC_CONTROL);
    pkt.extend_from_slice(&[0x09, 0x0c]);
    pkt.extend_from_slice(&[0x00; 7]);
    pkt.extend_from_slice(&timestamp_us.to_be_bytes());
    pkt
}

// ─── Metadata packet ─────────────────────────────────────────────────────────

/// Build the 09 0a channel metadata packet.
///
/// Wire format (confirmed from captures):
///   - JSON is a bare array: [{"c":1,"l":"Ch 1"}, ...]
///   - No wrapping object, no "id" field — device identity comes from the token
///   - Length encoded as 2-byte big-endian at bytes 18-19, zero pad at byte 20
///   - Length value covers token (16 bytes) + JSON payload
pub fn build_metadata_packet_with_labels(
    device_name_token: &[u8; 16],
    num_channels: usize,
    _node_id: &str,
    labels: &[String],
) -> Vec<u8> {
    let mut json = String::from("[");
    for ch in 0..num_channels {
        if ch > 0 { json.push(','); }
        let label = labels.get(ch).cloned().unwrap_or_else(|| local_channel_label(ch));
        json.push_str(&format!(r#"{{"c":{},"l":"{}"}}"#, ch + 1, json_escape(&label)));
    }
    json.push(']');

    let json_bytes = json.as_bytes();
    let body_len = 16usize + json_bytes.len();
    let len16 = body_len as u16;

    let mut pkt = rtp_header(SSRC_METADATA);
    pkt.extend_from_slice(&[0x09, 0x0a]);
    pkt.extend_from_slice(&[0x00, 0x01, 0x01, 0x00]); // flags
    pkt.push(((len16 >> 8) & 0xff) as u8);             // byte 18: len high
    pkt.push((len16 & 0xff) as u8);                    // byte 19: len low
    pkt.push(0x00);                                     // byte 20: padding
    pkt.extend_from_slice(device_name_token);
    pkt.extend_from_slice(json_bytes);
    pkt
}

pub fn build_metadata_packet(device_name_token: &[u8; 16], num_channels: usize, node_id: &str) -> Vec<u8> {
    let labels: Vec<String> = (0..num_channels).map(local_channel_label).collect();
    build_metadata_packet_with_labels(device_name_token, num_channels, node_id, &labels)
}

// ─── Metadata parsing ─────────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RemoteMetadata {
    pub node_id: Option<String>,
    pub channels: usize,
    pub labels: Vec<String>,
}

impl RemoteMetadata {
    pub fn display_name(&self) -> &str {
        self.node_id.as_deref().unwrap_or("unknown device")
    }
}

#[derive(Debug)]
pub enum HandshakePacket {
    Probe { sender_token: [u8; 16], expected_peer: [u8; 16] },
    Accept { token: [u8; 16] },
    Confirm { token: [u8; 16] },
    Metadata(RemoteMetadata),
    RttPing { timestamp_us: u64 },
    RttPong { timestamp_us: u64 },
}

pub fn parse_json_string_field(json: &str, field: &str) -> Option<String> {
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

pub fn parse_channel_metadata(json: &str) -> RemoteMetadata {
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

pub fn parse_handshake_packet(data: &[u8]) -> Option<HandshakePacket> {
    if data.len() < 14 || data[0] != 0x80 || data[1] != 0x69 { return None; }
    let ssrc = be_u32_at(data, 8)?;
    let kind = data.get(12..14)?;
    match (ssrc, kind) {
        (SSRC_CONTROL, [0x09, 0x07]) if data.len() >= 53 => {
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
            // Length field: 2-byte big-endian at bytes 18-19 (wire-confirmed).
            let body_len = ((data[18] as usize) << 8) | data[19] as usize;
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
        (SSRC_CONTROL, [0x09, 0x0b]) if data.len() >= 29 => {
            let ts = u64::from_be_bytes(data.get(21..29)?.try_into().ok()?);
            Some(HandshakePacket::RttPing { timestamp_us: ts })
        }
        (SSRC_CONTROL, [0x09, 0x0c]) if data.len() >= 29 => {
            let ts = u64::from_be_bytes(data.get(21..29)?.try_into().ok()?);
            Some(HandshakePacket::RttPong { timestamp_us: ts })
        }
        _ => None,
    }
}

// ─── Token derivation ────────────────────────────────────────────────────────

pub fn derive_link_token(my_name: &str, remote_name: &str, password: Option<&str>) -> [u8; 16] {
    let (a, b) = if my_name <= remote_name { (my_name, remote_name) } else { (remote_name, my_name) };
    match password.filter(|p| !p.is_empty()) {
        Some(pw) => derive_token_from_text(&format!("{a}:{b}:{pw}")),
        None      => derive_token_from_text(&format!("{a}:{b}")),
    }
}

pub fn derive_token_from_text(text: &str) -> [u8; 16] {
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

pub fn parse_token_arg(value: &str) -> Result<[u8; 16]> {
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

pub fn token_to_hex(token: &[u8; 16]) -> String {
    token.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn split_host_port(value: &str) -> String {
    value.strip_suffix(&format!(":{PORT}")).unwrap_or(value).to_string()
}
