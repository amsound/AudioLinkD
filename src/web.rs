use std::collections::{HashMap, VecDeque};
use std::net::UdpSocket;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
    Json, Router,
};
use cpal::traits::HostTrait;
use serde::{Deserialize, Serialize};
use crate::constants::*;
use crate::state::*;
use crate::packet::{
    build_metadata_packet_with_labels, local_channel_label, now_millis, token_to_hex,
    RemoteMetadata,
    parse_token_arg, split_host_port, derive_link_token, derive_token_from_text,
};
use crate::persistence::{
    load_persisted_state, save_persisted_config, save_persisted_routes,
    load_persisted_labels, PersistedRuntimeConfig,
};
use crate::routing::{
    apply_routes_to_masks, apply_routes_to_tx_sources, matrix_for_state,
    load_persisted_routes, tx_source_sample, source_code_from_bit_index,
};
use crate::audio::{peak_dbfs_from_peak, scan_audio_devices_once, sleep_until, ALSA_PERIOD};
use crate::interfaces::{list_network_interfaces, NetworkInterface};
use crate::jitter::effective_receive_buffer_ms;

// ─── Request / response types ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct RoutesRequest {
    pub routes: Vec<Route>,
    pub monitor_mode: Option<MonitorMode>,
}

#[derive(Debug, Deserialize)]
pub struct PresetRequest { pub name: String }

#[derive(Debug, Deserialize)]
pub struct LocalLabelsRequest { pub labels: Vec<String> }

#[derive(Debug, Deserialize)]
pub struct SetupApplyRequest {
    pub remote: String,
    pub remote_device_name: String,
    pub link_password: Option<String>,
    pub node_id: String,
    pub token: Option<String>,
    pub channels: usize,
    pub opus_bitrate_per_channel: u32,
    pub receive_buffer_ms: u32,
    pub rendezvous_url: Option<String>,
    pub phase_lock: Option<bool>,
    pub encoder_mode: Option<String>,
    pub bind_addr: Option<String>,
    pub selected_input_device: Option<String>,
    pub selected_output_device: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SetupApplyResponse { pub status: String, pub command: Vec<String> }

#[derive(Debug, Serialize)]
pub struct StatusResponse {
    pub node_id: String,
    pub uptime_seconds: u64,
    pub peer_status: PeerStatus,
    pub monitor_mode: MonitorMode,
    pub local_channels: usize,
    pub local_input_channels: usize,
    pub remote_channels: usize,
    pub send_enabled: bool,
    pub recv_enabled: bool,
    pub remote: Option<RemoteMetadata>,
    pub runtime: RuntimeSummary,
    pub last_control_age_ms: u64,
    pub last_audio_age_ms: u64,
    pub remote_conflict: Option<String>,
    pub discovered_peer_addr: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct DeviceIpResponse { pub ip: String, pub port: u16 }

// ─── Handlers ────────────────────────────────────────────────────────────────

pub async fn index_handler(State(_state): State<WebState>) -> Html<String> {
    // NOTE: The full single-file HTML/JS is embedded here.
    // The interface dropdown additions from the session are included.
    Html(include_str!("ui.html").to_string())
}

pub async fn status_handler(State(state): State<WebState>) -> Json<StatusResponse> {
    let now_ms = now_millis();
    let last_control = state.last_control_ms.load(Ordering::Relaxed);
    let last_audio   = state.last_audio_ms.load(Ordering::Relaxed);
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
        last_audio_age_ms:   if last_audio   == 0 { 0 } else { now_ms.saturating_sub(last_audio) },
        remote_conflict,
        discovered_peer_addr: state.established_peer_addr.lock().ok()
            .and_then(|g| *g).map(|a| a.ip().to_string()),
    })
}

pub async fn routes_get_handler(State(state): State<WebState>) -> Json<MatrixResponse> {
    Json(matrix_for_state(&state))
}

pub async fn routes_post_handler(
    State(state): State<WebState>,
    Json(req): Json<RoutesRequest>,
) -> impl IntoResponse {
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

pub async fn local_labels_post_handler(
    State(state): State<WebState>,
    Json(req): Json<LocalLabelsRequest>,
) -> impl IntoResponse {
    let mut labels: Vec<String> = req.labels.into_iter()
        .take(state.local_channels)
        .enumerate()
        .map(|(idx, label)| {
            let trimmed = label.trim();
            if trimmed.is_empty() { local_channel_label(idx) }
            else { trimmed.chars().take(48).collect() }
        })
        .collect();
    while labels.len() < state.local_channels {
        labels.push(local_channel_label(labels.len()));
    }
    if let Ok(mut current) = state.local_labels.lock() { *current = labels.clone(); }
    let mut persisted = load_persisted_state();
    persisted.config.channel_labels = Some(labels.clone());
    crate::persistence::save_persisted_state(&persisted);
    if state.handshake_connected.load(Ordering::Relaxed) {
        let pkt = build_metadata_packet_with_labels(
            &state.device_name_token, state.local_channels, &state.node_id, &labels);
        if let Err(e) = state.metadata_socket.send(&pkt) {
            tracing::warn!("Metadata resend after label edit failed: {e}");
        } else {
            tracing::info!("Metadata: resent 09 0a after local channel label edit");
        }
    }
    Json(matrix_for_state(&state)).into_response()
}

pub async fn preset_save_handler(
    State(state): State<WebState>, Json(req): Json<PresetRequest>,
) -> impl IntoResponse {
    let routes = state.routes.lock().map(|r| r.clone()).unwrap_or_default();
    if let Ok(mut presets) = state.presets.lock() { presets.insert(req.name.clone(), routes); }
    (StatusCode::OK, format!("saved preset '{}'", req.name))
}

pub async fn preset_recall_handler(
    State(state): State<WebState>, Json(req): Json<PresetRequest>,
) -> impl IntoResponse {
    let routes = state.presets.lock().ok().and_then(|p| p.get(&req.name).cloned());
    match routes {
        Some(routes) => {
            apply_routes_to_masks(&routes, &state.output_route_masks);
            apply_routes_to_tx_sources(&routes, &state.tx_tone_source_for_send);
            if let Ok(mut current) = state.routes.lock() {
                *current = routes.clone(); save_persisted_routes(&current);
            }
            Json(matrix_for_state(&state)).into_response()
        }
        None => (StatusCode::NOT_FOUND, format!("preset '{}' not found", req.name)).into_response(),
    }
}

pub async fn setup_apply_handler(
    State(state): State<WebState>, Json(req): Json<SetupApplyRequest>,
) -> impl IntoResponse {
    let remote              = req.remote.trim();
    let remote_device_name  = req.remote_device_name.trim();
    let node_id             = req.node_id.trim();
    if node_id.is_empty() {
        return (StatusCode::BAD_REQUEST, "Device name is required").into_response();
    }
    if req.channels == 0 || req.channels > MAX_CHANNELS {
        return (StatusCode::BAD_REQUEST, format!("Network send channels must be 1-{MAX_CHANNELS}")).into_response();
    }
    let link_password = req.link_password.as_deref().unwrap_or("").trim();
    let (token_hex, _token_derived) = if let Some(explicit) = req.token.as_deref().filter(|t| !t.trim().is_empty()) {
        match parse_token_arg(explicit.trim()) {
            Ok(token) => (token_to_hex(&token), false),
            Err(e) => return (StatusCode::BAD_REQUEST, format!("Invalid explicit token: {e}")).into_response(),
        }
    } else if remote_device_name.is_empty() {
        (token_to_hex(&DEFAULT_SHARED_TOKEN), false)
    } else {
        let token = derive_link_token(node_id, remote_device_name, if link_password.is_empty() { None } else { Some(link_password) });
        (token_to_hex(&token), true)
    };
    if !(8_000..=512_000).contains(&req.opus_bitrate_per_channel) {
        return (StatusCode::BAD_REQUEST, "Opus bitrate must be 8000-512000 bits/sec per channel").into_response();
    }
    let receive_buffer_ms   = req.receive_buffer_ms.clamp(MIN_LATENCY_MS, MAX_LATENCY_MS);
    let effective_buffer_ms = effective_receive_buffer_ms(receive_buffer_ms);
    let _guard = match state.restart_lock.try_lock() {
        Ok(g) => g,
        Err(_) => return (StatusCode::CONFLICT, "Engine rebuild is already in progress").into_response(),
    };

    let mut args = vec![
        "bidir".to_string(),
        "--channels".to_string(), req.channels.to_string(),
        "--id".to_string(), node_id.to_string(),
        "--token".to_string(), token_hex.clone(),
        "--bitrate".to_string(), req.opus_bitrate_per_channel.to_string(),
        "--latency-ms".to_string(), receive_buffer_ms.to_string(),
    ];
    if !remote_device_name.is_empty() {
        args.insert(1, remote_device_name.to_string());
        args.insert(1, "--remote-name".to_string());
    }
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
    if let Some(ref addr) = req.bind_addr.as_deref().filter(|a| !a.trim().is_empty() && *a != "0.0.0.0") {
        args.push("--interface".to_string());
        args.push(addr.to_string());
    }
    if let Some(ref dev) = req.selected_input_device.as_deref().filter(|d| !d.is_empty()) {
        args.push("--input-device".to_string());
        args.push(dev.to_string());
    }
    if let Some(ref dev) = req.selected_output_device.as_deref().filter(|d| !d.is_empty()) {
        args.push("--output-device".to_string());
        args.push(dev.to_string());
    }
    if state.runtime.fixed_jitter { args.push("--fixed-jitter".to_string()); }
    let phase_lock = req.phase_lock.unwrap_or(state.runtime.phase_lock);
    if !phase_lock { args.push("--no-phase-lock".to_string()); }
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
    let mut command_preview = vec![exe.display().to_string()];
    let mut hide_next = false;
    for arg in &args {
        if hide_next { command_preview.push("<token hidden>".to_string()); hide_next = false; }
        else {
            if arg == "--token" || arg == "--link-password" { hide_next = true; }
            command_preview.push(arg.clone());
        }
    }

    save_persisted_config(PersistedRuntimeConfig {
        remote: if remote.is_empty() { None } else { Some(split_host_port(remote)) },
        remote_device_name: if remote_device_name.is_empty() { None } else { Some(remote_device_name.to_string()) },
        link_password: if link_password.is_empty() { None } else { Some(link_password.to_string()) },
        node_id: Some(node_id.to_string()),
        token_hex: Some(token_hex.clone()),
        channels: Some(req.channels),
        opus_bitrate_per_channel: Some(req.opus_bitrate_per_channel),
        latency_ms: Some(receive_buffer_ms),
        fixed_jitter: Some(state.runtime.fixed_jitter),
        phase_lock: Some(phase_lock),
        encoder_mode: Some(encoder_mode),
        channel_labels: None,
        rendezvous_url: req.rendezvous_url.clone().filter(|u| !u.trim().is_empty()),
        bind_addr: req.bind_addr.clone().filter(|a| !a.trim().is_empty() && a != "0.0.0.0"),
        selected_input_device: req.selected_input_device.clone().filter(|d| !d.is_empty()),
        selected_output_device: req.selected_output_device.clone().filter(|d| !d.is_empty()),
    });

    let role = if remote.is_empty() { "responder" } else { "initiator" };
    tracing::warn!(
        "Setup Apply: role={role} remote_device={remote_device_name} remote_host={} \
         node={} channels={} bitrate={} receive_buffer={}ms effective={}ms",
        if remote.is_empty() { "<blank>" } else { remote },
        node_id, req.channels, req.opus_bitrate_per_channel, receive_buffer_ms, effective_buffer_ms
    );

    std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(250));
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            let err = std::process::Command::new(&exe).args(&args).exec();
            eprintln!("exec failed: {err}");
            std::process::exit(1);
        }
        #[cfg(not(unix))]
        {
            match std::process::Command::new(&exe).args(&args).spawn() {
                Ok(_) => std::process::exit(0),
                Err(e) => { eprintln!("engine rebuild spawn failed: {e}"); std::process::exit(1); }
            }
        }
    });

    Json(SetupApplyResponse { status: "rebuilding".to_string(), command: command_preview }).into_response()
}

pub async fn stats_handler(State(state): State<WebState>) -> Json<UiStats> {
    let mut s = state.stats.lock().map(|s| s.clone()).unwrap_or_default();
    let rx_n = state.remote_channels.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
    s.tx_peak_dbfs    = state.meters.snapshot_tx(state.local_channels);
    s.input_peak_dbfs = state.meters.snapshot_input(state.local_input_channels);
    s.rx_peak_dbfs    = state.meters.snapshot_rx(rx_n);
    s.monitor_peak_dbfs = state.meters.snapshot_monitor();
    Json(s)
}

pub async fn peers_handler(State(state): State<WebState>) -> Json<serde_json::Value> {
    let metadata = state.remote_metadata.lock().ok().and_then(|m| m.clone());
    Json(serde_json::json!([{ "id": "remote", "status": state.peer_status(), "metadata": metadata }]))
}

pub async fn streams_handler(State(state): State<WebState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "outgoing": [{ "id": "stream:0", "name": format!("{} Send", state.node_id), "channels": state.local_channels }],
        "incoming": state.remote_metadata.lock().ok().and_then(|m| m.clone()),
    }))
}

pub async fn devices_handler() -> Json<DeviceResponse> {
    // Live scan on every request — reflects the actual current device state
    // including any sample rate changes made by try_force_device_sample_rate().
    Json(crate::audio::scan_audio_devices_once())
}

pub async fn config_handler() -> Json<crate::persistence::PersistedUiState> {
    Json(crate::persistence::load_persisted_state())
}

pub async fn interfaces_handler() -> Json<Vec<NetworkInterface>> {
    Json(list_network_interfaces())
}

pub fn get_local_ip() -> String {
    std::net::UdpSocket::bind("0.0.0.0:0")
        .and_then(|s| { s.connect("8.8.8.8:53")?; Ok(s) })
        .and_then(|s| s.local_addr())
        .map(|a| a.ip().to_string())
        .unwrap_or_else(|_| "unknown".to_string())
}

pub async fn device_ip_handler() -> Json<DeviceIpResponse> {
    Json(DeviceIpResponse { ip: get_local_ip(), port: PORT })
}

pub async fn remotestatus_handler() -> Json<serde_json::Value> {
    Json(serde_json::json!({ "status": 0 }))
}

pub async fn events_handler(ws: WebSocketUpgrade, State(state): State<WebState>) -> impl IntoResponse {
    ws.on_upgrade(move |socket| events_socket(socket, state))
}

async fn events_socket(mut socket: WebSocket, state: WebState) {
    loop {
        let mut stats = state.stats.lock().map(|s| s.clone()).unwrap_or_default();
        let rx_n = state.remote_channels.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
        stats.tx_peak_dbfs      = state.meters.snapshot_tx(state.local_channels);
        stats.rx_peak_dbfs      = state.meters.snapshot_rx(rx_n);
        stats.monitor_peak_dbfs = state.meters.snapshot_monitor();
        let payload = serde_json::json!({
            "status": state.peer_status(),
            "matrix": matrix_for_state(&state),
            "stats":  stats,
        });
        if socket.send(Message::Text(payload.to_string().into())).await.is_err() { break; }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

// ─── Control-only metering (web-first mode) ───────────────────────────────────

pub fn spawn_control_only_metering(state: &WebState) {
    let meters           = Arc::clone(&state.meters);
    let tx_source_masks  = Arc::clone(&state.tx_tone_source_for_send);
    let ui_stats         = Arc::clone(&state.stats);
    let send_channels    = state.local_channels.min(MAX_CHANNELS);
    let input_channels   = state.local_input_channels.min(MAX_CHANNELS);

    std::thread::spawn(move || {
        let host = cpal::default_host();
        let input_rings: Arc<Mutex<Vec<VecDeque<f32>>>> = Arc::new(Mutex::new(
            (0..input_channels).map(|_| VecDeque::with_capacity(CAP_RING_SIZE)).collect(),
        ));

        let _input_stream: Option<cpal::Stream> = if input_channels > 0 {
            use cpal::traits::{DeviceTrait, StreamTrait};
            match host.default_input_device() {
                Some(in_device) => {
                    let in_config = cpal::StreamConfig {
                        channels: input_channels as u16,
                        sample_rate: cpal::SampleRate(SAMPLE_RATE),
                        buffer_size: ALSA_PERIOD,
                    };
                    let rings_cb   = Arc::clone(&input_rings);
                    let meters_cb  = Arc::clone(&meters);
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
                                tracing::info!("Control input metering active; no network engine running yet");
                                Some(stream)
                            }
                        }
                        Err(e) => { tracing::warn!("Control input meter unavailable: {e}"); None }
                    }
                }
                None => None,
            }
        } else { None };

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
                let mask = tx_source_masks.get(ch)
                    .map(|v| v.load(Ordering::Relaxed) as u64)
                    .unwrap_or(0);
                let mut frame = vec![0.0f32; FRAME_SAMPLES];
                let mut active_sources = 0usize;
                for bit in 0..64 {
                    if (mask & (1u64 << bit)) == 0 { continue; }
                    let Some(source_code) = source_code_from_bit_index(bit) else { continue; };
                    use crate::constants::{TX_SRC_EBU_L, TX_SRC_EBU_R, TX_SRC_INPUT_BASE};
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
                    stats.tx_peak_dbfs    = meters.snapshot_tx(send_channels);
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

// ─── Web state factory (web-first / no engine mode) ──────────────────────────

pub fn control_web_state(
    node_id: String,
    shared_token: [u8; 16],
    send_channels: usize,
    opus_bitrate_per_channel: u32,
    jitter: JitterConfig,
    encoder_mode: EncoderMode,
    rendezvous_url: Option<String>,
) -> anyhow::Result<WebState> {
    let send_channels       = send_channels.clamp(1, MAX_CHANNELS);
    let handshake_connected = Arc::new(AtomicBool::new(false));
    let last_control_ms     = Arc::new(AtomicU64::new(0));
    let last_audio_ms       = Arc::new(AtomicU64::new(0));
    let remote_channels     = Arc::new(AtomicUsize::new(0));
    let remote_metadata     = Arc::new(Mutex::new(None));
    let monitor_mode_atomic = Arc::new(AtomicU8::new(MonitorMode::PatchMatrix.as_u8()));
    let output_route_masks  = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    let tx_tone_source_for_send = Arc::new((0..MAX_CHANNELS).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>());
    let local_labels        = Arc::new(Mutex::new(load_persisted_labels(send_channels)));
    let presets             = Arc::new(Mutex::new(HashMap::new()));
    let mut initial_stats   = UiStats::default();
    initial_stats.target_ms = jitter.target_delay_ms;
    let stats               = Arc::new(Mutex::new(initial_stats));
    let meters              = Arc::new(MeterBank::new());
    let devices             = Arc::new(scan_audio_devices_once());
    let local_input_channels = if devices.default_input_channels == 0 { 0 } else { devices.default_input_channels.min(MAX_CHANNELS) };
    let initial_routes      = load_persisted_routes(local_input_channels, send_channels);
    apply_routes_to_masks(&initial_routes, &output_route_masks);
    apply_routes_to_tx_sources(&initial_routes, &tx_tone_source_for_send);
    let routes              = Arc::new(Mutex::new(initial_routes));
    let metadata_socket     = Arc::new(UdpSocket::bind("0.0.0.0:0")?);
    let state = WebState {
        started_at: Instant::now(),
        node_id: node_id.clone(),
        local_channels: send_channels,
        local_input_channels,
        send_enabled: true, recv_enabled: true,
        handshake_connected,
        last_control_ms, last_audio_ms,
        remote_channels, remote_metadata,
        monitor_mode: monitor_mode_atomic,
        output_route_masks, tx_tone_source_for_send,
        local_labels, metadata_socket,
        device_name_token: derive_token_from_text(&node_id),
        routes, presets, stats, meters,
        runtime: RuntimeSummary {
            mode: "bidirectional-ready".into(),
            remote_host: String::new(), remote_device_name: String::new(),
            source: "Matrix".into(), codec: "Opus".into(),
            opus_bitrate_per_channel, frame_ms: 20,
            tx_channels: send_channels, token_configured: false,
            token_hint: token_to_hex(&shared_token),
            token_hex: token_to_hex(&shared_token),
            send_enabled: true, recv_enabled: true,
            latency_ms: jitter.configured_delay_ms,
            effective_latency_ms: jitter.target_delay_ms,
            fixed_jitter: !jitter.adaptive, phase_lock: jitter.phase_lock,
            encoder_mode: encoder_mode.as_str().to_string(),
            link_password_configured: false, rendezvous_url: rendezvous_url.clone().unwrap_or_default(),
            bind_addr: "0.0.0.0".into(),
            selected_input_device: None,
            selected_output_device: None,
            web_note: "Web control running. Configure a remote device and apply to start the audio engine.".into(),
        },
        devices,
        restart_lock: Arc::new(Mutex::new(())),
        established_peer_addr: Arc::new(Mutex::new(None)),
        rtt_us10: Arc::new(AtomicU32::new(0)),
        remote_conflict: Arc::new(Mutex::new(None)),
    };
    spawn_control_only_metering(&state);
    Ok(state)
}

// ─── Server ───────────────────────────────────────────────────────────────────

pub fn spawn_web_ui(addr: String, state: WebState) {
    std::thread::spawn(move || {
        let rt = match tokio::runtime::Builder::new_multi_thread().enable_all().build() {
            Ok(rt) => rt,
            Err(e) => { tracing::error!("Web UI runtime failed: {e}"); return; }
        };
        rt.block_on(async move {
            let app = Router::new()
                .route("/",                     get(index_handler))
                .route("/api/status",           get(status_handler))
                .route("/api/audio/devices",    get(devices_handler))  // live scan, no State needed
                .route("/api/streams",          get(streams_handler).post(streams_handler))
                .route("/api/peers",            get(peers_handler))
                .route("/api/peers/connect",    post(status_handler))
                .route("/api/routes",           get(routes_get_handler).post(routes_post_handler))
                .route("/api/local-labels",     post(local_labels_post_handler))
                .route("/api/setup/apply",      post(setup_apply_handler))
                .route("/api/presets/save",     post(preset_save_handler))
                .route("/api/presets/recall",   post(preset_recall_handler))
                .route("/api/stats",            get(stats_handler))
                .route("/api/remotestatus",     get(remotestatus_handler))
                .route("/api/device-ip",        get(device_ip_handler))
                .route("/api/config",           get(config_handler))
                .route("/api/interfaces",       get(interfaces_handler))
                .route("/api/events",           get(events_handler))
                .with_state(state);
            let listener = match tokio::net::TcpListener::bind(&addr).await {
                Ok(l) => l,
                Err(e) => { tracing::error!("Web UI bind failed on {addr}: {e}"); return; }
            };
            tracing::info!("Web UI listening on http://{addr}");
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!("Web UI server stopped: {e}");
            }
        });
    });
}
