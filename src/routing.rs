use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::constants::*;
use crate::state::{Endpoint, MatrixResponse, Route, WebState};
use crate::packet::local_channel_label;
use crate::persistence::load_persisted_state;

// ─── Endpoint channel parsing ────────────────────────────────────────────────

/// Parse a simple `"{prefix}{channel}"` endpoint ID.
/// Used for: `"input:N"`, `"output:N"`.
pub fn parse_endpoint_channel(id: &str, prefix: &str) -> Option<usize> {
    id.strip_prefix(prefix)?.parse::<usize>().ok().filter(|&ch| ch < MAX_CHANNELS)
}

/// Parse a `"peer:{remote_index}:ch:{channel}"` receive endpoint ID.
/// Returns `(remote_index, channel)`.
pub fn parse_peer_endpoint(id: &str) -> Option<(usize, usize)> {
    let rest = id.strip_prefix("peer:")?;
    let colon = rest.find(':')?;
    let idx = rest[..colon].parse::<usize>().ok()?;
    let ch_str = rest[colon + 1..].strip_prefix("ch:")?;
    let ch = ch_str.parse::<usize>().ok().filter(|&c| c < MAX_CHANNELS)?;
    Some((idx, ch))
}

/// Parse a `"stream:{remote_index}:ch:{channel}"` send endpoint ID.
/// Returns `(remote_index, channel)`.
pub fn parse_stream_endpoint(id: &str) -> Option<(usize, usize)> {
    let rest = id.strip_prefix("stream:")?;
    let colon = rest.find(':')?;
    let idx = rest[..colon].parse::<usize>().ok()?;
    let ch_str = rest[colon + 1..].strip_prefix("ch:")?;
    let ch = ch_str.parse::<usize>().ok().filter(|&c| c < MAX_CHANNELS)?;
    Some((idx, ch))
}

// ─── Route validation ────────────────────────────────────────────────────────

/// Returns true if a persisted route is valid for the current runtime config.
///
/// Send routes:  source must be a known physical input or EBU tone;
///               destination must be a stream endpoint within send_channels.
/// Receive routes: source must be any peer endpoint (any remote index);
///                 destination must be output:0 or output:1.
///
pub fn route_valid_for_runtime(route: &Route, local_inputs: usize, send_channels: usize) -> bool {
    // Send route: destination is a network send channel.
    if let Some((_remote_idx, dst)) = parse_stream_endpoint(&route.destination) {
        if dst >= send_channels { return false; }
        if route.source == "ebu:l" || route.source == "ebu:r" { return true; }
        return parse_endpoint_channel(&route.source, "input:")
            .map(|ch| ch < local_inputs)
            .unwrap_or(false);
    }
    // Receive route: destination is a physical output.
    if let Some(dst) = parse_endpoint_channel(&route.destination, "output:") {
        if dst >= 2 { return false; }
        return parse_peer_endpoint(&route.source).is_some();
    }
    false
}

pub fn load_persisted_routes(local_inputs: usize, send_channels: usize) -> Vec<Route> {
    load_persisted_state()
        .routes
        .into_iter()
        .filter(|r| route_valid_for_runtime(r, local_inputs, send_channels))
        .collect()
}

pub fn apply_routes_to_masks(routes: &[Route], masks: &[AtomicU64; 2], channels_per_remote: usize) {
    let mut out_masks = [0u64; 2];
    for route in routes {
        let Some((remote_idx, src_ch)) = parse_peer_endpoint(&route.source) else { continue; };
        let Some(dst) = parse_endpoint_channel(&route.destination, "output:") else { continue; };
        // Flat bit index: remote_idx * channels_per_remote + src_ch
        let bit = remote_idx * channels_per_remote + src_ch;
        if dst < 2 && bit < 64 { out_masks[dst] |= 1u64 << bit; }
    }
    masks[0].store(out_masks[0], Ordering::Relaxed);
    masks[1].store(out_masks[1], Ordering::Relaxed);
}

pub fn tx_source_code_from_endpoint(id: &str) -> Option<usize> {
    match id {
        "ebu:l" => Some(TX_SRC_EBU_L),
        "ebu:r" => Some(TX_SRC_EBU_R),
        _ => parse_endpoint_channel(id, "input:").map(|ch| TX_SRC_INPUT_BASE + ch),
    }
}

pub fn tx_source_bit(source_code: usize) -> Option<u64> {
    match source_code {
        TX_SRC_EBU_L => Some(1u64 << 0),
        TX_SRC_EBU_R => Some(1u64 << 1),
        code if code >= TX_SRC_INPUT_BASE && code < TX_SRC_INPUT_BASE + 62 => {
            Some(1u64 << (2 + (code - TX_SRC_INPUT_BASE)))
        }
        _ => None,
    }
}

pub fn source_code_from_bit_index(bit: usize) -> Option<usize> {
    match bit {
        0 => Some(TX_SRC_EBU_L),
        1 => Some(TX_SRC_EBU_R),
        b if b >= 2 && b < 64 => Some(TX_SRC_INPUT_BASE + (b - 2)),
        _ => None,
    }
}

/// Apply send routes to the per-channel transmit source masks.
/// Accepts any remote index in the stream destination — for single-remote
/// operation only index 0 will appear; multi-remote adds further indices
/// in Stage 3 once per-remote tx_sources are introduced.
pub fn apply_routes_to_tx_sources(routes: &[Route], tx_sources: &[AtomicUsize]) {
    let mut masks = vec![0u64; tx_sources.len()];
    for route in routes {
        let Some(src_code) = tx_source_code_from_endpoint(&route.source) else { continue; };
        let Some(bit) = tx_source_bit(src_code) else { continue; };
        let Some((_remote_idx, dst)) = parse_stream_endpoint(&route.destination) else { continue; };
        if dst < masks.len() { masks[dst] |= bit; }
    }
    for (slot, mask) in tx_sources.iter().zip(masks.into_iter()) {
        slot.store(mask as usize, Ordering::Relaxed);
    }
}

// ─── Tone generators ─────────────────────────────────────────────────────────

pub fn sine_at(sample_index: u64, freq_hz: f32, amplitude: f32) -> f32 {
    let cycles = (sample_index as f64 * freq_hz as f64 / SAMPLE_RATE as f64).fract();
    (amplitude as f64 * (cycles * std::f64::consts::TAU).sin()) as f32
}

pub fn periodic_pos_s(sample_index: u64, period_s: f64) -> f64 {
    let period_samples = (SAMPLE_RATE as f64 * period_s).round() as u64;
    (sample_index % period_samples) as f64 / SAMPLE_RATE as f64
}

pub fn tx_source_sample(source_code: usize, sample_index: u64, _active_rotating_tone: usize) -> f32 {
    match source_code {
        TX_SRC_EBU_L => {
            let pos = periodic_pos_s(sample_index, 3.0);
            if pos < 0.25 { 0.0 } else { sine_at(sample_index, 1000.0, TONE_AMPLITUDE) }
        }
        TX_SRC_EBU_R => sine_at(sample_index, 1000.0, TONE_AMPLITUDE),
        _ => 0.0,
    }
}

pub fn matrix_for_state(state: &WebState) -> MatrixResponse {
    let local_labels = state.local_labels.lock().map(|l| l.clone()).unwrap_or_default();
    let remote = state.remote_metadata.lock().ok().and_then(|m| m.clone());
    let remembered_remote_channels = state.remote_channels.load(Ordering::Relaxed).min(MAX_CHANNELS);
    let remote_channels = remote.as_ref()
        .map(|m| m.channels.min(MAX_CHANNELS))
        .unwrap_or(remembered_remote_channels);
    let device_name = remote.as_ref()
        .and_then(|m| m.node_id.clone())
        .unwrap_or_else(|| "Connected Device".to_string());

    let mut sources = Vec::new();
    sources.push(Endpoint { id: "ebu:l".into(), label: "EBU L".into(), kind: "test_tone".into() });
    sources.push(Endpoint { id: "ebu:r".into(), label: "EBU R".into(), kind: "test_tone".into() });
    for ch in 0..state.local_input_channels {
        sources.push(Endpoint {
            id: format!("input:{ch}"),
            label: format!("Local Input {}", ch + 1),
            kind: "physical_input".into(),
        });
    }
    // Remote index 0 for Stage 2. Stage 3 iterates all remotes.
    for ch in 0..remote_channels {
        let label = remote.as_ref()
            .and_then(|m| m.labels.get(ch).cloned())
            .unwrap_or_else(|| format!("Ch {}", ch + 1));
        sources.push(Endpoint {
            id: format!("peer:0:ch:{ch}"),
            label: format!("{device_name} \u{2022} {label}"),
            kind: "network_receive".into(),
        });
    }

    let mut destinations = vec![
        Endpoint { id: "output:0".into(), label: "Local Output 1".into(), kind: "physical_output".into() },
        Endpoint { id: "output:1".into(), label: "Local Output 2".into(), kind: "physical_output".into() },
    ];
    for ch in 0..state.local_channels {
        let label = local_labels.get(ch).cloned().unwrap_or_else(|| local_channel_label(ch));
        destinations.push(Endpoint {
            id: format!("stream:0:ch:{ch}"),
            label: format!("Send {} \u{2014} {}", ch + 1, label),
            kind: "network_send".into(),
        });
    }

    let routes = state.routes.lock().map(|r| r.clone()).unwrap_or_default();
    MatrixResponse { monitor_mode: state.monitor_mode(), sources, destinations, routes }
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Route;

    fn r(src: &str, dst: &str) -> Route {
        Route { source: src.to_string(), destination: dst.to_string() }
    }

    // ── parse_peer_endpoint ──────────────────────────────────────────────────

    // T01 — current format parses correctly
    #[test]
    fn t01_parse_peer_current_format() {
        assert_eq!(parse_peer_endpoint("peer:0:ch:3"), Some((0, 3)));
        assert_eq!(parse_peer_endpoint("peer:1:ch:7"), Some((1, 7)));
        assert_eq!(parse_peer_endpoint("peer:2:ch:0"), Some((2, 0)));
    }

    // T03 — channel >= MAX_CHANNELS rejected
    #[test]
    fn t03_parse_peer_rejects_out_of_range_channel() {
        assert_eq!(parse_peer_endpoint("peer:0:ch:64"), None);
        assert_eq!(parse_peer_endpoint("peer:0:ch:255"), None);
    }

    // T04 — non-peer IDs return None
    #[test]
    fn t04_parse_peer_rejects_wrong_prefix() {
        assert_eq!(parse_peer_endpoint("input:0"), None);
        assert_eq!(parse_peer_endpoint("stream:0:ch:0"), None);
        assert_eq!(parse_peer_endpoint("output:0"), None);
    }

    // ── parse_stream_endpoint ────────────────────────────────────────────────

    // T05 — stream endpoint parses correctly
    #[test]
    fn t05_parse_stream_endpoint() {
        assert_eq!(parse_stream_endpoint("stream:0:ch:0"), Some((0, 0)));
        assert_eq!(parse_stream_endpoint("stream:1:ch:15"), Some((1, 15)));
    }

    // T06 — channel >= MAX_CHANNELS rejected
    #[test]
    fn t06_parse_stream_rejects_out_of_range() {
        assert_eq!(parse_stream_endpoint("stream:0:ch:64"), None);
    }

    // T07 — non-stream IDs return None
    #[test]
    fn t07_parse_stream_rejects_wrong_prefix() {
        assert_eq!(parse_stream_endpoint("peer:0:ch:0"), None);
        assert_eq!(parse_stream_endpoint("output:0"), None);
    }

    // ── route_valid_for_runtime ──────────────────────────────────────────────

    // T08 — valid send route: physical input → stream ch
    #[test]
    fn t08_valid_send_route_input() {
        assert!(route_valid_for_runtime(&r("input:0", "stream:0:ch:0"), 2, 8));
        assert!(route_valid_for_runtime(&r("input:1", "stream:0:ch:3"), 2, 8));
    }

    // T09 — valid send route: EBU tone → stream ch
    #[test]
    fn t09_valid_send_route_ebu() {
        assert!(route_valid_for_runtime(&r("ebu:l", "stream:0:ch:0"), 0, 8));
        assert!(route_valid_for_runtime(&r("ebu:r", "stream:0:ch:1"), 0, 8));
    }

    // T10 — send route rejected: input index >= local_inputs
    #[test]
    fn t10_send_route_rejected_input_out_of_range() {
        assert!(!route_valid_for_runtime(&r("input:2", "stream:0:ch:0"), 2, 8));
    }

    // T11 — send route rejected: destination channel >= send_channels
    #[test]
    fn t11_send_route_rejected_dst_out_of_range() {
        assert!(!route_valid_for_runtime(&r("input:0", "stream:0:ch:8"), 2, 8));
    }

    // T12 — valid receive route: peer ch → output (current format)
    #[test]
    fn t12_valid_receive_route_current_format() {
        assert!(route_valid_for_runtime(&r("peer:0:ch:0", "output:0"), 2, 8));
        assert!(route_valid_for_runtime(&r("peer:1:ch:3", "output:1"), 2, 8));
    }

    // T14 — receive route rejected: output index >= 2
    #[test]
    fn t14_receive_route_rejected_output_out_of_range() {
        assert!(!route_valid_for_runtime(&r("peer:0:ch:0", "output:2"), 2, 8));
    }

    // T15 — completely unknown route rejected
    #[test]
    fn t15_unknown_route_rejected() {
        assert!(!route_valid_for_runtime(&r("unknown:0", "unknown:1"), 2, 8));
    }

    // ── apply_routes_to_masks ────────────────────────────────────────────────

    use std::sync::atomic::AtomicU64;

    // T16 — current format sets correct bit in output mask
    #[test]
    fn t16_apply_masks_current_format() {
        let masks: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
        let routes = vec![r("peer:0:ch:3", "output:0")];
        apply_routes_to_masks(&routes, &masks, 8);
        assert_eq!(masks[0].load(Ordering::Relaxed), 1u64 << 3);
        assert_eq!(masks[1].load(Ordering::Relaxed), 0);
    }

    // T18 — multiple routes accumulate correctly
    #[test]
    fn t18_apply_masks_multiple_routes() {
        let masks: [AtomicU64; 2] = [AtomicU64::new(0), AtomicU64::new(0)];
        let routes = vec![
            r("peer:0:ch:0", "output:0"),
            r("peer:0:ch:1", "output:0"),
            r("peer:0:ch:2", "output:1"),
        ];
        apply_routes_to_masks(&routes, &masks, 8);
        assert_eq!(masks[0].load(Ordering::Relaxed), 0b11);
        assert_eq!(masks[1].load(Ordering::Relaxed), 1u64 << 2);
    }

    // T19 — empty routes clears masks
    #[test]
    fn t19_apply_masks_empty_clears() {
        let masks: [AtomicU64; 2] = [AtomicU64::new(0xffff), AtomicU64::new(0xffff)];
        apply_routes_to_masks(&[], &masks, 8);
        assert_eq!(masks[0].load(Ordering::Relaxed), 0);
        assert_eq!(masks[1].load(Ordering::Relaxed), 0);
    }

    // ── apply_routes_to_tx_sources ───────────────────────────────────────────

    use std::sync::atomic::AtomicUsize;

    // T20 — send route sets correct source bit on correct channel slot
    #[test]
    fn t20_apply_tx_sources_basic() {
        let tx: Vec<AtomicUsize> = (0..8).map(|_| AtomicUsize::new(0)).collect();
        let routes = vec![r("input:0", "stream:0:ch:2")];
        apply_routes_to_tx_sources(&routes, &tx);
        // input:0 → TX_SRC_INPUT_BASE + 0 → bit index 2 → bit value 4
        let expected_bit = 1u64 << 2; // bit 2 = input:0 (after ebu:l=bit0, ebu:r=bit1)
        assert_eq!(tx[2].load(Ordering::Relaxed), expected_bit as usize);
        assert_eq!(tx[0].load(Ordering::Relaxed), 0);
    }

    // T21 — EBU routes set correct bits
    #[test]
    fn t21_apply_tx_sources_ebu() {
        let tx: Vec<AtomicUsize> = (0..4).map(|_| AtomicUsize::new(0)).collect();
        let routes = vec![
            r("ebu:l", "stream:0:ch:0"),
            r("ebu:r", "stream:0:ch:1"),
        ];
        apply_routes_to_tx_sources(&routes, &tx);
        assert_eq!(tx[0].load(Ordering::Relaxed), 1); // bit 0 = ebu:l
        assert_eq!(tx[1].load(Ordering::Relaxed), 2); // bit 1 = ebu:r
    }

    // T22 — channel index >= tx_sources.len() is silently ignored (no panic, no write)
    // Note: apply_routes_to_tx_sources slots by channel index, not remote index.
    // stream:0:ch:4 and stream:1:ch:4 both resolve to dst=4 which is out of range
    // for a 4-slot array. Remote index is parsed but not used as a multiplier
    // until Stage 3 introduces per-remote tx arrays.
    #[test]
    fn t22_apply_tx_sources_ignores_out_of_range_channel() {
        let tx: Vec<AtomicUsize> = (0..4).map(|_| AtomicUsize::new(0)).collect();
        let routes = vec![r("ebu:l", "stream:0:ch:4")]; // ch:4 is out of range for 4-slot array
        apply_routes_to_tx_sources(&routes, &tx);
        for i in 0..4 { assert_eq!(tx[i].load(Ordering::Relaxed), 0); }
    }
}
