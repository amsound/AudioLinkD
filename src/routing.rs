use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use crate::constants::*;
use crate::state::{Endpoint, MatrixResponse, Route, WebState};
use crate::packet::local_channel_label;
use crate::persistence::load_persisted_state;

// ─── Endpoint channel parsing ────────────────────────────────────────────────

pub fn parse_endpoint_channel(id: &str, prefix: &str) -> Option<usize> {
    id.strip_prefix(prefix)?.parse::<usize>().ok().filter(|&ch| ch < MAX_CHANNELS)
}

// ─── Route validation ────────────────────────────────────────────────────────

pub fn route_valid_for_runtime(route: &Route, local_inputs: usize, send_channels: usize) -> bool {
    if let Some(dst) = parse_endpoint_channel(&route.destination, "stream:0:ch:") {
        if dst >= send_channels { return false; }
        if route.source == "ebu:l" || route.source == "ebu:r" { return true; }
        return parse_endpoint_channel(&route.source, "input:")
            .map(|ch| ch < local_inputs)
            .unwrap_or(false);
    }
    if let Some(dst) = parse_endpoint_channel(&route.destination, "output:") {
        if dst >= 2 { return false; }
        return parse_endpoint_channel(&route.source, "peer:remote:ch:").is_some();
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

pub fn default_routes_for_channels(_channels: usize) -> Vec<Route> {
    Vec::new()
}

// ─── Mask application ────────────────────────────────────────────────────────

pub fn apply_routes_to_masks(routes: &[Route], masks: &[AtomicU64; 2]) {
    let mut out_masks = [0u64; 2];
    for route in routes {
        let Some(src) = parse_endpoint_channel(&route.source, "peer:remote:ch:") else { continue; };
        let Some(dst) = parse_endpoint_channel(&route.destination, "output:") else { continue; };
        if dst < 2 { out_masks[dst] |= 1u64 << src; }
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

pub fn apply_routes_to_tx_sources(routes: &[Route], tx_sources: &[AtomicUsize]) {
    let mut masks = vec![0u64; tx_sources.len()];
    for route in routes {
        let Some(src_code) = tx_source_code_from_endpoint(&route.source) else { continue; };
        let Some(bit) = tx_source_bit(src_code) else { continue; };
        let Some(dst) = parse_endpoint_channel(&route.destination, "stream:0:ch:") else { continue; };
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

pub fn tx_source_peak_estimate(source_code: usize, frame_start_sample: u64, _active_rotating_tone: usize) -> f32 {
    let mut peak = 0.0f32;
    for offset in (0..FRAME_SAMPLES).step_by(12) {
        peak = peak.max(tx_source_sample(source_code, frame_start_sample + offset as u64, 0).abs());
    }
    peak
}

// ─── Matrix view ─────────────────────────────────────────────────────────────

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
    for ch in 0..remote_channels {
        let label = remote.as_ref()
            .and_then(|m| m.labels.get(ch).cloned())
            .unwrap_or_else(|| format!("Ch {}", ch + 1));
        sources.push(Endpoint {
            id: format!("peer:remote:ch:{ch}"),
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
