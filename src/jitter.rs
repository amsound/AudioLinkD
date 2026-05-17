use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};
use ringbuf::traits::Observer;
use crate::constants::*;
use crate::audio::decode_or_plc_xfade;

// ─── RTP helpers ─────────────────────────────────────────────────────────────

pub fn timestamp_elapsed_samples(newer: u32, older: u32) -> u32 {
    newer.wrapping_sub(older)
}

pub fn rtp_timestamp_at_or_before(ts: u32, reference: u32) -> bool {
    reference.wrapping_sub(ts) < 0x8000_0000
}

pub fn rtp_seq_looks_reset(prev: u16, next: u16) -> bool {
    prev != next && prev.wrapping_sub(next) < 0x8000
}

pub fn latency_ms_to_samples(ms: u32) -> u32 {
    ((ms as u64 * SAMPLE_RATE as u64) / 1000) as u32
}

pub fn effective_receive_buffer_ms(configured_ms: u32) -> u32 {
    configured_ms
        .clamp(MIN_LATENCY_MS, MAX_LATENCY_MS)
        .max(MIN_EFFECTIVE_RX_BUFFER_MS)
}

pub fn ring_fill_ms<T: Observer>(rings: &[T], active_channels: usize) -> usize {
    if active_channels == 0 { return 0; }
    let min_fill = rings[..active_channels]
        .iter()
        .map(|r| r.occupied_len())
        .min()
        .unwrap_or(0);
    min_fill * 1000 / SAMPLE_RATE as usize
}

// ─── FrameGroup ───────────────────────────────────────────────────────────────

pub struct FrameGroup {
    pub packets: Vec<Option<Vec<u8>>>,
    pub expected: HashSet<u8>,
    pub received_at: Instant,
}

impl FrameGroup {
    pub fn new(_timestamp: u32, expected_channels: usize) -> Self {
        let expected = (0..expected_channels.min(MAX_CHANNELS))
            .map(|ch| ch as u8)
            .collect();
        Self {
            packets: vec![None; MAX_CHANNELS],
            expected,
            received_at: Instant::now(),
        }
    }

    pub fn insert(&mut self, channel: usize, payload: &[u8]) {
        if channel < MAX_CHANNELS {
            self.packets[channel] = Some(payload.to_vec());
        }
    }

    pub fn complete(&self) -> bool {
        self.expected.iter().all(|&ch| self.packets[ch as usize].is_some())
    }

    pub fn timed_out(&self) -> bool {
        self.received_at.elapsed() >= Duration::from_millis(PHASE_LOCK_TIMEOUT_MS)
    }
}

// ─── Phase-locked drain ───────────────────────────────────────────────────────

/// Drain the oldest-first BTreeMap of frame groups into decoded PCM.
///
/// Decodes complete groups immediately. Incomplete groups are decoded
/// (with Opus PLC for missing channels) after the PHASE_LOCK_TIMEOUT_MS.
///
/// Uses decode_or_plc_xfade for smooth real/PLC transitions.
///
/// Returns (groups_decoded, plc_channel_count, last_drained_timestamp).
/// The last_drained_timestamp is used as a high-watermark to discard
/// late-arriving packets that have already been PLC'd — prevents double-decode.
#[allow(clippy::too_many_arguments)]
pub fn drain_phase_locked_groups(
    groups: &mut BTreeMap<u32, FrameGroup>,
    decoders: &mut [opus::Decoder],
    remote_channels: usize,
    decoded: &mut [Vec<f32>],
    pcm: &mut [f32],
    xfade_tails: &mut Vec<Vec<f32>>,
    xfade_was_plc: &mut Vec<bool>,
    on_group: &mut dyn FnMut(&[Vec<f32>], usize),
) -> (usize, usize, Option<u32>) {
    let active = remote_channels.min(MAX_CHANNELS);
    if active == 0 { return (0, 0, None); }

    let mut output_groups = 0usize;
    let mut plc_channels  = 0usize;
    let mut last_drained_ts: Option<u32> = None;

    loop {
        let ready_ts = match groups.iter().next() {
            Some((&ts, group)) if group.complete() || group.timed_out() => ts,
            _ => break,
        };
        let Some(group) = groups.remove(&ready_ts) else { continue; };
        for ch in 0..active {
            let was_real = decode_or_plc_xfade(
                &mut decoders[ch],
                group.packets[ch].as_deref(),
                pcm,
                &mut xfade_tails[ch],
                &mut xfade_was_plc[ch],
            );
            if !was_real { plc_channels += 1; }
            decoded[ch].copy_from_slice(pcm);
        }
        on_group(decoded, active);
        output_groups += 1;
        last_drained_ts = Some(ready_ts);
    }

    (output_groups, plc_channels, last_drained_ts)
}
