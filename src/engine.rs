use std::collections::{BTreeMap, HashMap, VecDeque};
use std::net::UdpSocket;
use std::sync::{
    atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::{Duration, Instant};
use anyhow::{anyhow, Result};
use cpal::traits::{DeviceTrait, HostTrait, StreamTrait};
use ringbuf::{traits::{Consumer, Observer, Producer, Split}, HeapRb};
use rubato::Resampler;

use crate::constants::*;
use crate::state::*;
use crate::packet::*;
use crate::persistence::*;
use crate::routing::*;
use crate::audio::*;
use crate::jitter::*;
use crate::interfaces::spawn_interface_monitor;
use crate::web::spawn_web_ui;

// ─── UDP disconnect (AF_UNSPEC) ───────────────────────────────────────────────

fn udp_disconnect_socket(socket: &UdpSocket) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            let mut sa: libc::sockaddr_storage = std::mem::zeroed();
            sa.ss_family = libc::AF_UNSPEC as libc::sa_family_t;
            let ptr = &sa as *const libc::sockaddr_storage as *const libc::sockaddr;
            let rc = libc::connect(
                socket.as_raw_fd(), ptr,
                std::mem::size_of::<libc::sockaddr>() as libc::socklen_t,
            );
            if rc == 0 { return Ok(()); }
            let rc2 = libc::connect(
                socket.as_raw_fd(), ptr,
                std::mem::size_of::<libc::sockaddr_storage>() as libc::socklen_t,
            );
            if rc2 == 0 { return Ok(()); }
            let err = std::io::Error::last_os_error();
            if err.kind() == std::io::ErrorKind::NotConnected { Ok(()) } else { Err(err) }
        }
    }
    #[cfg(not(unix))]
    { let _ = socket; Ok(()) }
}

// ─── run_bidir ────────────────────────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
pub fn run_bidir(
    remote_host: &str,
    remote_device_name: &str,
    num_channels: usize,
    send_enabled: bool,
    recv_enabled: bool,
    device_name_token: [u8; 16],
    shared_token: [u8; 16],
    node_id: String,
    jitter: JitterConfig,
    web_addr: Option<String>,
    opus_bitrate_per_channel: u32,
    rendezvous_url: Option<String>,
    encoder_mode: EncoderMode,
    bind_addr: String,
    selected_input_device: Option<String>,
    selected_output_device: Option<String>,
) -> Result<()> {
    if num_channels == 0 || num_channels > MAX_CHANNELS {
        return Err(anyhow!("Channel count must be 1–{MAX_CHANNELS}"));
    }
    let is_initiator = !remote_host.trim().is_empty();
    let remote_addr_str = format!("{remote_host}:{PORT}");

    let socket = {
        let mut sock = None;
        let mut last_err = None;
        for attempt in 0..10 {
            match UdpSocket::bind(format!("{bind_addr}:{PORT}")) {
                Ok(s) => { sock = Some(Arc::new(s)); break; }
                Err(e) => {
                    if attempt > 0 { tracing::warn!("Port {PORT} busy (attempt {}/10), retrying…", attempt + 1); }
                    last_err = Some(e);
                    std::thread::sleep(Duration::from_millis(500));
                }
            }
        }
        sock.ok_or_else(|| anyhow!("Could not bind {bind_addr}:{PORT}: {}", last_err.unwrap()))?
    };
    if is_initiator { socket.connect(&remote_addr_str)?; }
    tracing::info!("Bound to {bind_addr}:{PORT}");

    #[cfg(unix)]
    if matches!(std::env::var("AUDIOLINK_DSCP_EF").ok().as_deref(),
        Some("1"|"true"|"yes"|"on")) {
        use std::os::unix::io::AsRawFd;
        let tos: libc::c_int = 0xb8;
        let rc = unsafe {
            libc::setsockopt(socket.as_raw_fd(), libc::IPPROTO_IP, libc::IP_TOS,
                &tos as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::c_int>() as libc::socklen_t)
        };
        if rc == 0 { tracing::info!("DSCP EF marking enabled"); }
        else { tracing::warn!("DSCP EF setsockopt failed: {}", std::io::Error::last_os_error()); }
    }

    let established_addr:   Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
    let punch_target:       Arc<Mutex<Option<std::net::SocketAddr>>> = Arc::new(Mutex::new(None));
    let receive_reset_epoch = Arc::new(AtomicU64::new(0));
    let handshake_connected = Arc::new(AtomicBool::new(false));
    let last_control_ms     = Arc::new(AtomicU64::new(0));
    let last_audio_ms       = Arc::new(AtomicU64::new(0));
    let remote_channels     = Arc::new(AtomicUsize::new(num_channels));
    let remote_metadata     = Arc::new(Mutex::new(None));
    let monitor_mode_atomic = Arc::new(AtomicU8::new(0u8)); // always PatchMatrix
    let output_route_masks  = Arc::new([AtomicU64::new(0), AtomicU64::new(0)]);
    let tx_tone_source_for_send = Arc::new(
        (0..MAX_CHANNELS).map(|_| AtomicUsize::new(0)).collect::<Vec<_>>()
    );
    let local_labels        = Arc::new(Mutex::new(load_persisted_labels(num_channels)));
    let presets             = Arc::new(Mutex::new(HashMap::new()));
    let mut initial_stats   = UiStats::default();
    initial_stats.target_ms = jitter.target_delay_ms;
    let ui_stats            = Arc::new(Mutex::new(initial_stats));
    let meters              = Arc::new(MeterBank::new());
    let devices             = Arc::new(scan_audio_devices_once());
    let local_input_channels = devices.default_input_channels.min(MAX_CHANNELS);
    let initial_routes      = load_persisted_routes(local_input_channels, num_channels);
    apply_routes_to_masks(&initial_routes, &output_route_masks);
    apply_routes_to_tx_sources(&initial_routes, &tx_tone_source_for_send);
    let routes              = Arc::new(Mutex::new(initial_routes));
    let rtt_us10            = Arc::new(AtomicU32::new(0));
    let remote_conflict: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

    let web_state = WebState {
        started_at: Instant::now(),
        node_id: node_id.clone(),
        local_channels: num_channels,
        local_input_channels,
        send_enabled, recv_enabled,
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
        device_name_token,
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
            send_enabled, recv_enabled,
            latency_ms: jitter.configured_delay_ms,
            effective_latency_ms: jitter.target_delay_ms,
            fixed_jitter: !jitter.adaptive,
            phase_lock: jitter.phase_lock,
            encoder_mode: encoder_mode.as_str().to_string(),
            link_password_configured: load_persisted_state()
                .config.link_password.map(|p| !p.is_empty()).unwrap_or(false),
            rendezvous_url: rendezvous_url.clone().unwrap_or_default(),
            bind_addr: bind_addr.clone(),
            selected_input_device: selected_input_device.clone(),
            selected_output_device: selected_output_device.clone(),
            web_note: "Setup Apply performs a controlled process rebuild.".into(),
        },
        devices: Arc::clone(&devices),
        restart_lock: Arc::new(Mutex::new(())),
        established_peer_addr: Arc::clone(&established_addr),
        rtt_us10: Arc::clone(&rtt_us10),
        actual_input_rate: Arc::new(AtomicU32::new(0)),
        actual_output_rate: Arc::new(AtomicU32::new(0)),
        remote_conflict: Arc::clone(&remote_conflict),
    };

    // Extract Arc refs before web_state is moved into spawn_web_ui
    let actual_input_rate  = Arc::clone(&web_state.actual_input_rate);
    let actual_output_rate = Arc::clone(&web_state.actual_output_rate);
    if let Some(addr) = web_addr { spawn_web_ui(addr, web_state); }

    tracing::info!(
        "Bidir: role={} remote_device={remote_device_name} remote_host={} \
         channels={num_channels} send={send_enabled} recv={recv_enabled} id={node_id} \
         bind={bind_addr} buffer={}ms effective={}ms adaptive={} phase_lock={} encoder={}",
        if is_initiator { "initiator" } else { "responder" },
        if is_initiator { &remote_addr_str } else { "<blank>" },
        jitter.configured_delay_ms, jitter.target_delay_ms,
        jitter.adaptive, jitter.phase_lock, encoder_mode.as_str()
    );

    spawn_interface_monitor(Arc::clone(&receive_reset_epoch), Arc::clone(&handshake_connected));

    // ── Handshake / keepalive / staleness watchdog ──────────────────────────
    {
        let socket_hs       = Arc::clone(&socket);
        let connected_hs    = Arc::clone(&handshake_connected);
        let established_hs  = Arc::clone(&established_addr);
        let punch_target_hs = Arc::clone(&punch_target);
        let last_control_hs = Arc::clone(&last_control_ms);
        let last_audio_hs   = Arc::clone(&last_audio_ms);
        let rtt_hs          = Arc::clone(&rtt_us10);
        let reset_epoch_hs  = Arc::clone(&receive_reset_epoch);
        let remote_name_hs  = remote_device_name.to_string();
        const STALE_MS: u64 = 15_000;

        std::thread::spawn(move || {
            let probe = build_probe_packet(&device_name_token, &shared_token);
            loop {
                let connected = connected_hs.load(Ordering::Relaxed);
                if connected {
                    let now_ms = now_millis();
                    let last_any = last_control_hs.load(Ordering::Relaxed)
                        .max(last_audio_hs.load(Ordering::Relaxed));
                    if last_any > 0 && now_ms.saturating_sub(last_any) > STALE_MS {
                        tracing::warn!("Stale: no packet for {}ms — resetting", now_ms.saturating_sub(last_any));
                        if !is_initiator {
                            udp_disconnect_socket(socket_hs.as_ref()).ok();
                            if let Ok(mut e) = established_hs.lock() { *e = None; }
                        }
                        connected_hs.store(false, Ordering::Relaxed);
                        last_control_hs.store(0, Ordering::Relaxed);
                        last_audio_hs.store(0, Ordering::Relaxed);
                        rtt_hs.store(0, Ordering::Relaxed);
                        reset_epoch_hs.fetch_add(1, Ordering::Relaxed);
                        std::thread::sleep(Duration::from_millis(2130));
                        continue;
                    }
                }
                let connected = connected_hs.load(Ordering::Relaxed);
                if is_initiator {
                    socket_hs.send(&probe).ok();
                    if connected { socket_hs.send(&build_rtt_ping(now_us())).ok(); }
                } else if connected {
                    socket_hs.send(&probe).ok();
                    socket_hs.send(&build_rtt_ping(now_us())).ok();
                } else {
                    match punch_target_hs.lock().ok().and_then(|g| *g) {
                        Some(addr) => { socket_hs.send_to(&probe, addr).ok(); }
                        None => { tracing::trace!("Responder: waiting for {remote_name_hs}"); }
                    }
                }
                std::thread::sleep(Duration::from_millis(2130));
            }
        });
    }

    // ── Rendezvous ─────────────────────────────────────────────────────────
    if let Some(rdv_url) = rendezvous_url.clone().filter(|u| !u.trim().is_empty()) {
        let rdv_base = {
            let u = rdv_url.trim_end_matches('/');
            if u.starts_with("http://") || u.starts_with("https://") { u.to_string() }
            else { format!("https://{u}") }
        };
        let reg_body = serde_json::json!({ "name": node_id, "port": PORT }).to_string();
        let con_body = serde_json::json!({ "my_name": node_id, "remote_name": remote_device_name }).to_string();
        {
            let base = rdv_base.clone();
            let socket_r = Arc::clone(&socket);
            let punch_r  = Arc::clone(&punch_target);
            let estab_r  = Arc::clone(&established_addr);
            let conn_r   = Arc::clone(&handshake_connected);
            let _node_r   = node_id.clone();
            let remote_r = remote_device_name.to_string();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(1500));
                loop {
                    match ureq::post(&format!("{base}/api/register"))
                        .set("Content-Type","application/json").send_string(&reg_body)
                    {
                        Ok(_) => {
                            if !is_initiator && !remote_r.is_empty() && !conn_r.load(Ordering::Relaxed) {
                                if let Ok(resp) = ureq::post(&format!("{base}/api/connect"))
                                    .set("Content-Type","application/json").send_string(&con_body)
                                {
                                    if let Ok(text) = resp.into_string() {
                                        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&text) {
                                            let addr_s = v["remote_addr"].as_str().unwrap_or("?");
                                            tracing::info!("rendezvous: connect → {remote_r} @ {addr_s}");
                                            if let Ok(addr) = addr_s.parse::<std::net::SocketAddr>() {
                                                if let Ok(mut t) = punch_r.lock() { *t = Some(addr); }
                                                if !conn_r.load(Ordering::Relaxed) {
                                                    if let Ok(mut e) = estab_r.lock() {
                                                        if e.is_some() { udp_disconnect_socket(socket_r.as_ref()).ok(); *e = None; }
                                                    }
                                                }
                                                socket_r.send_to(&build_probe_packet(&device_name_token, &shared_token), addr).ok();
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Err(e) => tracing::warn!("rendezvous: register failed: {e}"),
                    }
                    std::thread::sleep(Duration::from_secs(10));
                }
            });
        }
        {
            let base     = rdv_base.clone();
            let node_e   = node_id.clone();
            let socket_e = Arc::clone(&socket);
            let punch_e  = Arc::clone(&punch_target);
            let estab_e  = Arc::clone(&established_addr);
            let conn_e   = Arc::clone(&handshake_connected);
            std::thread::spawn(move || {
                loop {
                    match ureq::get(&format!("{base}/api/events/{node_e}")).call() {
                        Ok(resp) if resp.status() == 200 => {
                            if let Ok(text) = resp.into_string() {
                                if let Ok(evt) = serde_json::from_str::<serde_json::Value>(&text) {
                                    let from_addr = evt["from_addr"].as_str().unwrap_or("");
                                    let from_name = evt["from_name"].as_str().unwrap_or("?");
                                    if from_addr.is_empty() { continue; }
                                    tracing::info!("rendezvous: inbound from {from_name} @ {from_addr}");
                                    if let Ok(addr) = from_addr.parse::<std::net::SocketAddr>() {
                                        if let Ok(mut t) = punch_e.lock() { *t = Some(addr); }
                                        if !conn_e.load(Ordering::Relaxed) {
                                            if let Ok(mut e) = estab_e.lock() {
                                                if e.is_some() { udp_disconnect_socket(socket_e.as_ref()).ok(); *e = None; }
                                            }
                                        }
                                        socket_e.send_to(&build_probe_packet(&device_name_token, &shared_token), addr).ok();
                                    }
                                }
                            }
                        }
                        Ok(resp) if resp.status() == 404 => {
                            // 404 = node not registered yet or unknown; wait before retry
                            std::thread::sleep(Duration::from_secs(5));
                        }
                        Ok(_) => {} // 204 = no events, reconnect immediately
                        Err(e) => { tracing::warn!("rendezvous: events poll: {e}"); std::thread::sleep(Duration::from_secs(5)); }
                    }
                    std::thread::sleep(Duration::from_millis(200));
                }
            });
        }
        tracing::info!("M9 rendezvous: registered with {rdv_url} as {node_id}");
    }

    let host = cpal::default_host();

    // ── Audio hardware initialisation (runs before network starts) ────────────
    // On Linux: suppress_alsa_stderr wraps all ALSA enumeration so the wall of
    // "cannot connect to JACK/PulseAudio/OSS" messages from libasound doesn't
    // flood the logs. Those messages are libasound probing virtual backends and
    // are completely harmless — they go to stderr directly, bypassing tracing.
    suppress_alsa_stderr(|| {
        use cpal::traits::HostTrait;
        let out_name = selected_output_device.as_deref()
            .filter(|n| !n.is_empty())
            .map(|n| n.to_string())
            .or_else(|| host.default_output_device().and_then(|d| d.name().ok()));
        let in_name = selected_input_device.as_deref()
            .filter(|n| !n.is_empty())
            .map(|n| n.to_string())
            .or_else(|| host.default_input_device().and_then(|d| d.name().ok()));
        if let Some(name) = out_name {
            try_force_device_sample_rate(&name, SAMPLE_RATE);
        }
        if let Some(name) = in_name {
            try_force_device_sample_rate(&name, SAMPLE_RATE);
        }
    });
    // Small settle time for CoreAudio to apply the rate change before we
    // open streams. 100ms is enough; the hardware switch is near-instant.
    #[cfg(target_os = "macos")]
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Helper: find a device by name, falling back to the default.
    // Used for both input and output selection.
    let find_output_device = |preferred: &Option<String>| -> Option<cpal::Device> {
        if let Some(name) = preferred.as_deref().filter(|n| !n.is_empty()) {
            if let Ok(mut devs) = host.output_devices() {
                if let Some(d) = devs.find(|d| d.name().ok().as_deref() == Some(name)) {
                    tracing::info!("Output: using selected device '{name}'");
                    return Some(d);
                }
                tracing::warn!("Output device '{name}' not found — falling back to default");
            }
        }
        host.default_output_device()
    };
    let find_input_device = |preferred: &Option<String>| -> Option<cpal::Device> {
        if let Some(name) = preferred.as_deref().filter(|n| !n.is_empty()) {
            if let Ok(mut devs) = host.input_devices() {
                if let Some(d) = devs.find(|d| d.name().ok().as_deref() == Some(name)) {
                    tracing::info!("Input: using selected device '{name}'");
                    return Some(d);
                }
                tracing::warn!("Input device '{name}' not found — falling back to default");
            }
        }
        host.default_input_device()
    };

    // input_ok: declared here so it's in scope for both recv and send pipelines
    let input_ok     = Arc::new(AtomicBool::new(true));
    let input_ok_err = Arc::clone(&input_ok);

    // ── Receive / playback pipeline ─────────────────────────────────────────
    let _out_stream: Option<cpal::Stream> = if recv_enabled {
        let out_device = suppress_alsa_stderr(|| find_output_device(&selected_output_device))
            .ok_or_else(|| anyhow!("No output device"))?;
        let (out_config, out_device_rate) = suppress_alsa_stderr(|| best_output_config(&out_device, 2));
        tracing::info!("Output: {} @ {}Hz stereo{}",
            out_device.name().unwrap_or_default(),
            out_device_rate,
            if out_device_rate != SAMPLE_RATE { " (resampling from 48kHz)" } else { "" });
        actual_output_rate.store(out_device_rate, Ordering::Relaxed);

        let prime_samples = ((jitter.target_delay_ms as usize * SAMPLE_RATE as usize) / 1000)
            .max(PRIME_SAMPLES);
        let ring_samples = (prime_samples * 2).max(PB_RING_SIZE);

        let mut pb_prods = Vec::with_capacity(MAX_CHANNELS);
        let mut pb_conss = Vec::with_capacity(MAX_CHANNELS);
        for _ in 0..MAX_CHANNELS {
            let (prod, cons) = HeapRb::<f32>::new(ring_samples).split();
            pb_prods.push(prod); pb_conss.push(cons);
        }



        let output_ok      = Arc::new(AtomicBool::new(true));
        let output_ok_play = Arc::clone(&output_ok);
        let output_ok_recv = Arc::clone(&output_ok);
        let started        = Arc::new(AtomicBool::new(false));
        let started_recv   = Arc::clone(&started);
        let started_play   = Arc::clone(&started);
        let flush_samples  = Arc::new(AtomicUsize::new(0));
        let flush_recv     = Arc::clone(&flush_samples);
        let flush_play     = Arc::clone(&flush_samples);
        let empty_cb       = Arc::new(AtomicU32::new(0));
        let empty_cb_play  = Arc::clone(&empty_cb);
        let empty_cb_recv  = Arc::clone(&empty_cb);
        let underflows     = Arc::new(AtomicUsize::new(0));
        let underflows_recv = Arc::clone(&underflows);
        let underflows_play = Arc::clone(&underflows);

        let socket_recv          = Arc::clone(&socket);
        let connected_recv       = Arc::clone(&handshake_connected);
        let connected_play       = Arc::clone(&handshake_connected);
        let last_ctrl_recv       = Arc::clone(&last_control_ms);
        let last_audio_recv      = Arc::clone(&last_audio_ms);
        let remote_ch_recv       = Arc::clone(&remote_channels);
        let remote_ch_play       = Arc::clone(&remote_channels);
        let remote_meta_recv     = Arc::clone(&remote_metadata);
        let route_masks_play     = Arc::clone(&output_route_masks);
        let meters_play          = Arc::clone(&meters);
        let meters_recv          = Arc::clone(&meters);
        let ui_stats_recv        = Arc::clone(&ui_stats);
        let local_labels_recv    = Arc::clone(&local_labels);
        let node_id_recv         = node_id.clone();
        let rtt_recv             = Arc::clone(&rtt_us10);
        let conflict_recv        = Arc::clone(&remote_conflict);
        let estab_recv           = Arc::clone(&established_addr);
        let punch_recv           = Arc::clone(&punch_target);
        let reset_epoch_recv     = Arc::clone(&receive_reset_epoch);
        let ring_overflows_recv  = Arc::new(AtomicUsize::new(0));
        let _ring_overflows_play  = Arc::clone(&ring_overflows_recv);

        // Receive / decode thread
        std::thread::spawn(move || {
            // Optional: try SCHED_FIFO for low-latency receive
            #[cfg(unix)]
            unsafe {
                let mut p: libc::sched_param = std::mem::zeroed();
                p.sched_priority = 20;
                let r = libc::pthread_setschedparam(libc::pthread_self(), libc::SCHED_FIFO, &p);
                if r != 0 { tracing::warn!("recv: SCHED_FIFO failed (errno {r}). Try: sudo setcap cap_sys_nice+eip ./audiolinkd"); }
                else { tracing::info!("recv: SCHED_FIFO priority 20 active"); }
            }
            socket_recv.set_read_timeout(Some(Duration::from_millis(1))).ok();

            let mut decoders    = fresh_opus_decoders();
            let mut resamplers  = fresh_asrc_resamplers();
            let mut xfade_tails:   Vec<Vec<f32>> = vec![Vec::new(); MAX_CHANNELS];
            let mut xfade_was_plc: Vec<bool>     = vec![false; MAX_CHANNELS];

            let mut buf           = vec![0u8; 65535];
            let mut pcm           = vec![0f32; FRAME_SAMPLES];
            let mut decoded_frame: Vec<Vec<f32>> = vec![vec![0.0; FRAME_SAMPLES]; MAX_CHANNELS];
            let mut last_remote_meta: Option<RemoteMetadata> = None;
            let mut groups: BTreeMap<u32, FrameGroup> = BTreeMap::new();
            let mut newest_ts: Option<u32>  = None;
            let mut last_seq = vec![None::<u16>; MAX_CHANNELS];
            let mut last_stats = Instant::now();
            let mut plc_total:      usize = 0;
            let mut lost_total:     usize = 0;
            let mut decoded_total:  usize = 0;
            let mut decoded_at_last:usize = 0;
            let mut media_total:    usize = 0;
            let mut media_at_last:  usize = 0;
            let mut rx_bytes_total: usize = 0;
            let mut rx_bytes_at:    usize = 0;
            let mut jitter_ts_prev: Option<u32>  = None;
            let mut jitter_arr_prev:Option<Instant> = None;
            let mut jitter_ms:      f64   = 0.0;
            const JWIN: usize = 300;
            let mut jitter_win: VecDeque<f64> = VecDeque::with_capacity(JWIN + 1);
            let mut underflows_at:  usize = 0;
            let mut plc_at:         usize = 0;
            let mut lost_at:        usize = 0;
            let mut overflows_at:   usize = 0;
            let mut last_ts_nonplc: Option<u32> = None;
            let mut correction:     f64   = 1.0;
            let mut integral:       f64   = 0.0;
            let mut integral_last = Instant::now();
            let mut last_drained_ts: Option<u32> = None;
            let mut observed_epoch = reset_epoch_recv.load(Ordering::Relaxed);

            macro_rules! reset_session {
                ($reason:expr, $clear_meta:expr) => {{
                    let active = remote_ch_recv.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
                    let old_fill = pb_prods[..active].iter().map(|p| p.occupied_len()).max().unwrap_or(0);
                    groups.clear(); newest_ts = None; last_drained_ts = None;
                    last_seq.fill(None);
                    jitter_ts_prev = None; jitter_arr_prev = None; jitter_ms = 0.0; jitter_win.clear();
                    last_ts_nonplc = None; integral = 0.0; correction = 1.0; integral_last = Instant::now();
                    decoders = fresh_opus_decoders(); resamplers = fresh_asrc_resamplers();
                    for t in xfade_tails.iter_mut() { t.clear(); }
                    xfade_was_plc.fill(false);
                    media_total = 0; media_at_last = 0;
                    rx_bytes_total = 0; rx_bytes_at = 0;
                    lost_total = 0; lost_at = 0;
                    plc_total = 0; plc_at = 0;
                    decoded_total = 0; decoded_at_last = 0;
                    underflows_at = underflows_recv.load(Ordering::Relaxed);
                    last_stats = Instant::now();
                    if $clear_meta { last_remote_meta = None; }
                    started_recv.store(false, Ordering::Relaxed);
                    empty_cb_recv.store(0, Ordering::Relaxed);
                    output_ok_recv.store(true, Ordering::Relaxed); // allow reconnect after device re-plug
                    if old_fill > 0 {
                        flush_recv.store(old_fill.saturating_add(FRAME_SAMPLES), Ordering::Relaxed);
                    }
                    tracing::info!(
                        "Session reset ({}) — flushing {}ms",
                        $reason, old_fill * 1000 / SAMPLE_RATE as usize
                    );
                }};
            }

            let responder_late_connect = |src: Option<std::net::SocketAddr>| -> bool {
                if is_initiator { return true; }
                let Some(src) = src else {
                    tracing::warn!("Responder: handshake had no source addr");
                    return false;
                };
                let mut estab = match estab_recv.lock() { Ok(g) => g, Err(_) => return false };
                match *estab {
                    Some(known) if known == src => true,
                    Some(known) => {
                        tracing::warn!("Conflict: packet from {src} but connected to {known}");
                        if let Ok(mut c) = conflict_recv.lock() { if c.is_none() { *c = Some(src.to_string()); } }
                        false
                    }
                    None => match socket_recv.connect(src) {
                        Ok(()) => {
                            tracing::info!("Responder: late-connected to {src}");
                            *estab = Some(src);
                            if let Ok(mut t) = punch_recv.lock() { *t = None; }
                            true
                        }
                        Err(e) => { tracing::error!("Responder: late-connect to {src} failed: {e}"); false }
                    }
                }
            };

            loop {
                let epoch = reset_epoch_recv.load(Ordering::Relaxed);
                if epoch != observed_epoch {
                    observed_epoch = epoch;
                    reset_session!("epoch/watchdog reset", true);
                }

                let result = if is_initiator {
                    socket_recv.recv(&mut buf).map(|n| (n, None))
                } else {
                    socket_recv.recv_from(&mut buf).map(|(n, a)| (n, Some(a)))
                };

                match result {
                    Ok((n, src)) => {
                        // Reject packets from unknown source once established (responder)
                        if !is_initiator {
                            if let Some(src_addr) = src {
                                if let Some(known) = estab_recv.lock().ok().and_then(|g| *g) {
                                    if known != src_addr {
                                        if let Ok(mut c) = conflict_recv.lock() { if c.is_none() { *c = Some(src_addr.to_string()); } }
                                        continue;
                                    }
                                }
                            }
                        }

                        if let Some(pkt) = parse_handshake_packet(&buf[..n]) {
                            match pkt {
                                HandshakePacket::Probe { sender_token, expected_peer }
                                    if expected_peer == shared_token =>
                                {
                                    if !responder_late_connect(src) { continue; }
                                    last_ctrl_recv.store(now_millis(), Ordering::Relaxed);
                                    let sender_name = last_remote_meta.as_ref()
                                        .and_then(|m| m.node_id.clone())
                                        .unwrap_or_else(|| token_to_hex(&sender_token));
                                    let already = connected_recv.load(Ordering::Relaxed);
                                    if !already {
                                        socket_recv.send(&build_accept_packet(&shared_token)).ok();
                                        socket_recv.send(&build_confirm_packet(&shared_token)).ok();
                                        let labels = local_labels_recv.lock()
                                            .map(|l| l.clone())
                                            .unwrap_or_else(|_| (0..num_channels).map(local_channel_label).collect());
                                        socket_recv.send(&build_metadata_packet_with_labels(
                                            &device_name_token, num_channels, &node_id_recv, &labels)).ok();
                                        connected_recv.store(true, Ordering::Relaxed);
                                        reset_session!("new handshake", true);
                                        tracing::info!("HS: probe from {sender_name} — sent accept/confirm/metadata");
                                        if let Ok(mut c) = conflict_recv.lock() { *c = None; }
                                    } else {
                                        // Keepalive: CONFIRM only (wire-confirmed correct)
                                        socket_recv.send(&build_confirm_packet(&shared_token)).ok();
                                        tracing::trace!("Keepalive probe from {sender_name} — sent confirm");
                                    }
                                }
                                HandshakePacket::Accept { token } if token == shared_token => {
                                    if !responder_late_connect(src) { continue; }
                                    last_ctrl_recv.store(now_millis(), Ordering::Relaxed);
                                    let labels = local_labels_recv.lock()
                                        .map(|l| l.clone())
                                        .unwrap_or_else(|_| (0..num_channels).map(local_channel_label).collect());
                                    socket_recv.send(&build_confirm_packet(&shared_token)).ok();
                                    socket_recv.send(&build_metadata_packet_with_labels(
                                        &device_name_token, num_channels, &node_id_recv, &labels)).ok();
                                    if !connected_recv.swap(true, Ordering::Relaxed) {
                                        reset_session!("accepted", true);
                                        tracing::info!("HS: accept received — sent confirm/metadata");
                                        if let Ok(mut c) = conflict_recv.lock() { *c = None; }
                                    }
                                }
                                HandshakePacket::Confirm { token } if token == shared_token => {
                                    if !responder_late_connect(src) { continue; }
                                    last_ctrl_recv.store(now_millis(), Ordering::Relaxed);
                                    if !connected_recv.swap(true, Ordering::Relaxed) {
                                        reset_session!("confirmed", true);
                                        tracing::info!("HS: confirm received");
                                        if let Ok(mut c) = conflict_recv.lock() { *c = None; }
                                    }
                                }
                                HandshakePacket::Metadata(meta) if meta.channels > 0 => {
                                    if !connected_recv.load(Ordering::Relaxed) { continue; }
                                    last_ctrl_recv.store(now_millis(), Ordering::Relaxed);
                                    let mut meta = meta;
                                    meta.channels = meta.channels.min(MAX_CHANNELS);
                                    if last_remote_meta.as_ref() == Some(&meta) { continue; }
                                    let old_ch = last_remote_meta.as_ref()
                                        .map(|m| m.channels)
                                        .unwrap_or_else(|| remote_ch_recv.load(Ordering::Relaxed));
                                    let ch_changed = meta.channels != old_ch;
                                    remote_ch_recv.store(meta.channels, Ordering::Relaxed);
                                    if ch_changed { reset_session!("channel layout changed", false); }
                                    tracing::info!("Metadata: {} — {} ch {:?}", meta.display_name(), meta.channels, meta.labels);
                                    if let Ok(mut m) = remote_meta_recv.lock() { *m = Some(meta.clone()); }
                                    last_remote_meta = Some(meta);
                                }
                                HandshakePacket::RttPing { timestamp_us } => {
                                    if !connected_recv.load(Ordering::Relaxed) { continue; }
                                    last_ctrl_recv.store(now_millis(), Ordering::Relaxed);
                                    socket_recv.send(&build_rtt_pong(timestamp_us)).ok();
                                }
                                HandshakePacket::RttPong { timestamp_us } => {
                                    if !connected_recv.load(Ordering::Relaxed) { continue; }
                                    last_ctrl_recv.store(now_millis(), Ordering::Relaxed);
                                    let now = now_us();
                                    if now >= timestamp_us {
                                        rtt_recv.store(((now - timestamp_us) * 10 / 1000) as u32, Ordering::Relaxed);
                                    }
                                }
                                _ => {}
                            }
                            continue;
                        }

                        if !is_initiator && !connected_recv.load(Ordering::Relaxed) { continue; }
                        let pkt = match parse_packet(&buf[..n]) {
                            Some(p) => { last_audio_recv.store(now_millis(), Ordering::Relaxed); p }
                            None => continue,
                        };
                        if flush_recv.load(Ordering::Relaxed) > 0 { continue; }

                        media_total += 1;
                        rx_bytes_total += n;
                        let arrival = Instant::now();
                        if Some(pkt.timestamp) != jitter_ts_prev {
                            if let (Some(prev_ts), Some(prev_arr)) = (jitter_ts_prev, jitter_arr_prev) {
                                let exp_ms = timestamp_elapsed_samples(pkt.timestamp, prev_ts) as f64
                                    * 1000.0 / SAMPLE_RATE as f64;
                                if exp_ms > 0.0 && exp_ms < 1000.0 {
                                    let arr_ms = arrival.duration_since(prev_arr).as_secs_f64() * 1000.0;
                                    let delta = (arr_ms - exp_ms).abs();
                                    jitter_ms += (delta - jitter_ms) / 16.0;
                                    if jitter_win.len() >= JWIN { jitter_win.pop_front(); }
                                    jitter_win.push_back(delta);
                                }
                            }
                            jitter_ts_prev = Some(pkt.timestamp);
                            jitter_arr_prev = Some(arrival);
                        }

                        let ch = pkt.channel as usize;
                        if ch >= MAX_CHANNELS { continue; }

                        if jitter.phase_lock {
                            if let Some(hwm) = last_drained_ts {
                                let rollback  = hwm.wrapping_sub(pkt.timestamp);
                                let behind    = rtp_timestamp_at_or_before(pkt.timestamp, hwm);
                                let seq_reset = last_seq[ch].map(|p| rtp_seq_looks_reset(p, pkt.seq)).unwrap_or(false);
                                let fresh_ts  = pkt.timestamp <= RTP_RESTART_LOW_TS_SAMPLES;
                                if behind && rollback > RTP_RESTART_ROLLBACK_SAMPLES && (seq_reset || fresh_ts) {
                                    tracing::warn!("RTP restart on ch{ch} — resetting session");
                                    reset_session!("RTP restart", false);
                                }
                            }
                        }
                        if let Some(prev) = last_seq[ch] {
                            let expected = prev.wrapping_add(1);
                            if pkt.seq != expected {
                                let gap = pkt.seq.wrapping_sub(expected) as usize;
                                if gap < 10_000 { lost_total += gap; }
                            }
                        }
                        last_seq[ch] = Some(pkt.seq);

                        let active = remote_ch_recv.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
                        newest_ts = Some(match newest_ts {
                            Some(prev) if timestamp_elapsed_samples(pkt.timestamp, prev) < 0x8000_0000 => pkt.timestamp,
                            Some(prev) => prev,
                            None => pkt.timestamp,
                        });

                        if jitter.phase_lock {
                            if let Some(hwm) = last_drained_ts {
                                if rtp_timestamp_at_or_before(pkt.timestamp, hwm) { continue; }
                            }
                            let group = groups.entry(pkt.timestamp)
                                .or_insert_with(|| FrameGroup::new(pkt.timestamp, active));
                            group.insert(ch, pkt.opus_payload);
                        } else {
                            if last_ts_nonplc != Some(pkt.timestamp) {
                                decoded_total += 1;
                                last_ts_nonplc = Some(pkt.timestamp);
                            }
                            let ok = decode_or_plc_xfade(
                                &mut decoders[ch], Some(pkt.opus_payload), &mut pcm,
                                &mut xfade_tails[ch], &mut xfade_was_plc[ch]);
                            if !ok { plc_total += 1; }
                            resamplers[ch].set_resample_ratio(correction, false).ok();
                            let resampled = resamplers[ch].process(&[&pcm], None).unwrap_or_else(|_| vec![pcm.to_vec()]);
                            let mut peak = 0.0f32;
                            for &s in resampled[0].iter() {
                                peak = peak.max(s.abs());
                                if pb_prods[ch].try_push(s).is_err() {
                                    ring_overflows_recv.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            meters_recv.set_rx_peak(ch, peak_dbfs_from_peak(peak));
                        }
                        empty_cb_recv.store(0, Ordering::Relaxed);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                           || e.kind() == std::io::ErrorKind::TimedOut => {}
                    Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => {
                        if connected_recv.swap(false, Ordering::Relaxed) {
                            tracing::info!("recv: remote gone (ECONNREFUSED)");
                        }
                        if !is_initiator {
                            udp_disconnect_socket(socket_recv.as_ref()).ok();
                            if let Ok(mut e) = estab_recv.lock() { *e = None; }
                        }
                        if started_recv.load(Ordering::Relaxed) { reset_session!("ECONNREFUSED", true); }
                        last_ctrl_recv.store(0, Ordering::Relaxed);
                        last_audio_recv.store(0, Ordering::Relaxed);
                        rtt_recv.store(0, Ordering::Relaxed);
                        std::thread::sleep(Duration::from_millis(100));
                    }
                    Err(e) => { tracing::error!("recv: {e}"); }
                }

                let active = remote_ch_recv.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);

                if jitter.phase_lock {
                    let ring_ov = Arc::clone(&ring_overflows_recv);
                    let meters_d = Arc::clone(&meters_recv);
                    let (out, plc, ts) = drain_phase_locked_groups(
                        &mut groups, &mut decoders, active,
                        &mut decoded_frame, &mut pcm,
                        &mut xfade_tails, &mut xfade_was_plc,
                        &mut |decoded, active_ch| {
                            for ch in 0..active_ch {
                                resamplers[ch].set_resample_ratio(correction, false).ok();
                                let resampled = resamplers[ch].process(&[&decoded[ch]], None)
                                    .unwrap_or_else(|_| vec![decoded[ch].to_vec()]);
                                let mut peak = 0.0f32;
                                for &s in resampled[0].iter() {
                                    peak = peak.max(s.abs());
                                    if pb_prods[ch].try_push(s).is_err() {
                                        ring_ov.fetch_add(1, Ordering::Relaxed);
                                    }
                                }
                                meters_d.set_rx_peak(ch, peak_dbfs_from_peak(peak));
                            }
                        },
                    );
                    decoded_total += out;
                    plc_total += plc;
                    if let Some(ts) = ts { last_drained_ts = Some(ts); }
                }

                let fill_ms     = ring_fill_ms(&pb_prods, active);
                let fill_err_ms = fill_ms as f64 - jitter.target_delay_ms as f64;

                if jitter.adaptive {
                    let dt = integral_last.elapsed().as_secs_f64();
                    if dt >= 0.2 {
                        integral += fill_err_ms * 0.000003 * dt;
                        integral = integral.clamp(-0.012, 0.012);
                        integral_last = Instant::now();
                    }
                } else {
                    integral = 0.0; integral_last = Instant::now();
                }
                correction = if jitter.adaptive {
                    (1.0 - fill_err_ms * 0.000010 - integral).clamp(0.980, 1.020)
                } else { 1.0 };

                if fill_ms > jitter.target_delay_ms as usize * 3 {
                    integral = 0.0; integral_last = Instant::now();
                    if output_ok_recv.load(Ordering::Relaxed) {
                        tracing::warn!("Buffer high: {}ms — clearing integral", fill_ms);
                    }
                }

                if !started_recv.load(Ordering::Relaxed)
                    && pb_prods[..active].iter().all(|p| p.occupied_len() >= prime_samples)
                {
                    started_recv.store(true, Ordering::Relaxed);
                    let actual = pb_prods[..active].iter().map(|p| p.occupied_len()).min().unwrap_or(0)
                        * 1000 / SAMPLE_RATE as usize;
                    tracing::info!("Primed: {active} ch — target {}ms actual {actual}ms — starting playback",
                        prime_samples * 1000 / SAMPLE_RATE as usize);
                }

                if last_stats.elapsed() >= STATS_LOG_INTERVAL {
                    let elapsed  = last_stats.elapsed().as_secs_f64().max(0.001);
                    let rx_fps   = (decoded_total - decoded_at_last) as f64 / elapsed;
                    decoded_at_last = decoded_total;
                    let med_delta = media_total.saturating_sub(media_at_last);
                    media_at_last = media_total;
                    let byt_delta = rx_bytes_total.saturating_sub(rx_bytes_at);
                    rx_bytes_at = rx_bytes_total;
                    let rx_mbps  = byt_delta as f64 * 8.0 / elapsed / 1_000_000.0;
                    let uf_now   = underflows_recv.load(Ordering::Relaxed);
                    let uf_delta = uf_now.saturating_sub(underflows_at);
                    underflows_at = uf_now;
                    let plc_delta  = plc_total.saturating_sub(plc_at); plc_at = plc_total;
                    let lost_delta = lost_total.saturating_sub(lost_at); lost_at = lost_total;
                    let loss_den   = med_delta.saturating_add(lost_delta);
                    let loss_pct   = if loss_den > 0 { lost_delta as f64 * 100.0 / loss_den as f64 } else { 0.0 };
                    let rtt_ms     = rtt_recv.load(Ordering::Relaxed) as f64 / 10.0;
                    let ov_now     = ring_overflows_recv.load(Ordering::Relaxed);
                    let ov_delta   = ov_now.saturating_sub(overflows_at); overflows_at = ov_now;
                    let jitter_p95 = if jitter_win.len() >= 10 {
                        let mut s: Vec<f64> = jitter_win.iter().copied().collect();
                        s.sort_unstable_by(|a,b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                        s[((s.len() as f64 * 0.95) as usize).min(s.len()-1)]
                    } else { jitter_ms };
                    let rec_buf_ms = ((jitter_p95 * 4.0).ceil() as u32).clamp(80, 500);
                    let ppm = ((correction - 1.0) * 1_000_000.0).round() as isize;
                    tracing::info!(
                        "RX ch={active} {:.3}Mbps loss={:.3}% p95={:.2}ms fill={}ms target={}ms fps={:.1} plc={} drift={}ppm rtt={:.1}ms ov={}",
                        rx_mbps, loss_pct, jitter_p95, fill_ms, jitter.target_delay_ms, rx_fps, plc_delta, ppm, rtt_ms, ov_delta
                    );
                    if let Ok(mut s) = ui_stats_recv.lock() {
                        s.channels = active; s.fill_ms = fill_ms;
                        s.target_ms = jitter.target_delay_ms; s.phase_lock = jitter.phase_lock;
                        s.queued_groups = groups.len(); s.decoded_fps = rx_fps;
                        s.output_underflows = uf_delta; s.plc_channels = plc_delta;
                        s.seq_missing = lost_delta; s.loss_percent = loss_pct;
                        s.jitter_ms = jitter_ms; s.jitter_p95_ms = jitter_p95;
                        s.recommended_buffer_ms = rec_buf_ms;
                        s.latency_ms = fill_ms; s.rx_mbps = rx_mbps;
                        s.drift_pressure_ppm = ppm; s.rtt_ms = rtt_ms;
                        s.one_way_latency_ms = rtt_ms / 2.0; s.ring_overflows = ov_delta;
                    }
                    last_stats = Instant::now();
                }
            }
        });

        // Output resampler: converts 48kHz frames to the device's native rate
        // when they differ (e.g. BT headphones that only do 44.1kHz).
        // Output callback
        // If the device rate differs from 48kHz, we need to resample.
        // The resampler and carry buffer are pre-allocated and moved into the closure —
        // no allocation happens in the hot path.
        // Strategy: produce a 48kHz stereo staging frame, resample it into out_carry,
        // then drain out_carry into the cpal buffer. Carry-over samples persist between
        // callbacks. The cpal buffer is always exactly filled.
        let mut out_resampler: Option<rubato::FastFixedIn<f32>> = make_io_resampler_n(SAMPLE_RATE, out_device_rate, 2);
        // Input chunk for resampler: one 20ms stereo frame at 48kHz = 960 * 2 samples
        // but Rubato works per-channel, so chunk is 960 samples.
        let out_resample_chunk = FRAME_SAMPLES; // 960 samples at 48kHz per channel
        // Pre-allocate carry buffer: 2 channels × max_output_frames
        let out_carry_capacity = if out_resampler.is_some() {
            (out_resample_chunk as f64 * out_device_rate as f64 / SAMPLE_RATE as f64 * 2.5) as usize * 2
        } else { 0 };
        let mut out_carry: Vec<f32> = Vec::with_capacity(out_carry_capacity);
        let mut out_staging_l = vec![0.0f32; out_resample_chunk];
        let mut out_staging_r = vec![0.0f32; out_resample_chunk];
        let _out_staging_pos = 0usize; // how many 48kHz frames we've written to staging
        let mut limiter = Limiter::new();
        let stream = out_device.build_output_stream(
            &out_config,
            move |data: &mut [f32], _| {
                // ── Resampled output path ──────────────────────────────────────────────
                // When the device runs at a rate other than 48kHz, we produce 48kHz
                // stereo staging frames, resample them, accumulate in out_carry,
                // then drain out_carry into the cpal buffer. This keeps the hot path
                // allocation-free — all buffers were pre-allocated above.
                if out_resampler.is_some() {
                    let mut pos = 0usize;
                    while pos < data.len() {
                        // Drain carry buffer first
                        if !out_carry.is_empty() {
                            let take = out_carry.len().min(data.len() - pos);
                            data[pos..pos + take].copy_from_slice(&out_carry[..take]);
                            out_carry.drain(..take);
                            pos += take;
                            continue;
                        }
                        // Need more: produce one 48kHz staging frame into out_staging_l/r.
                        // We reuse the full callback logic on a FRAME_SAMPLES-sized view.
                        // For brevity: fill with silence when not started.
                        let active_r = remote_ch_play.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
                        let playing_r = connected_play.load(Ordering::Relaxed)
                            && started_play.load(Ordering::Relaxed);
                        if !playing_r {
                            for s in out_carry.iter_mut() { *s = 0.0; }
                            data[pos..].fill(0.0);
                            return;
                        }
                        let left_m  = route_masks_play[0].load(Ordering::Relaxed);
                        let right_m = route_masks_play[1].load(Ordering::Relaxed);
                        for i in 0..out_resample_chunk {
                            let mut samples = [0.0f32; MAX_CHANNELS];
                            for ch in 0..active_r {
                                samples[ch] = pb_conss[ch].try_pop().unwrap_or(0.0);
                            }
                            let (mut l, mut r) = (0.0f32, 0.0f32);
                            let (mut lc, mut rc) = (0u32, 0u32);
                            for ch in 0..active_r {
                                let bit = 1u64 << ch;
                                if left_m  & bit != 0 { l += samples[ch]; lc += 1; }
                                if right_m & bit != 0 { r += samples[ch]; rc += 1; }
                            }
                            out_staging_l[i] = if lc > 0 { l / lc as f32 } else { l };
                            out_staging_r[i] = if rc > 0 { r / rc as f32 } else { r };
                        }
                        // Apply limiter and metering to staging frame (interleaved)
                        let mut interleaved: Vec<f32> = out_staging_l.iter()
                            .zip(out_staging_r.iter())
                            .flat_map(|(&a, &b)| [a, b])
                            .collect();
                        limiter.process(&mut interleaved);
                        // Resample each channel
                        let l_in: Vec<f32> = interleaved.iter().step_by(2).copied().collect();
                        let r_in: Vec<f32> = interleaved.iter().skip(1).step_by(2).copied().collect();
                        if let Some(ref mut rs) = out_resampler {
                            // 2-channel Rubato: process L and R in one call
                            if let Ok(out_ch) = rs.process(&[&l_in, &r_in], None) {
                                for (l, r) in out_ch[0].iter().zip(out_ch[1].iter()) {
                                    out_carry.push(*l);
                                    out_carry.push(*r);
                                }
                            }
                        }
                    }
                    return;
                }
                // ── Normal 48kHz path (no resampling needed) ──────────────────────────
                let active = remote_ch_play.load(Ordering::Relaxed).clamp(1, MAX_CHANNELS);
                let playing = connected_play.load(Ordering::Relaxed) && started_play.load(Ordering::Relaxed);
                if !playing {
                    let mut remaining = flush_play.load(Ordering::Relaxed);
                    if remaining > 0 {
                        for frame in data.chunks_exact_mut(2) {
                            for ch in 0..active { let _ = pb_conss[ch].try_pop(); }
                            frame[0] = 0.0; frame[1] = 0.0;
                            remaining = remaining.saturating_sub(1);
                            if remaining == 0 { break; }
                        }
                        flush_play.store(remaining, Ordering::Relaxed);
                    }
                    data.fill(0.0); return;
                }
                let gain_snapshot = meters_play.snapshot_rx(active);
                let left_mask  = route_masks_play[0].load(Ordering::Relaxed);
                let right_mask = route_masks_play[1].load(Ordering::Relaxed);
                let mut any_sample = false;
                let mut underflowed = false;
                let mut rx_peaks = [0.0f32; MAX_CHANNELS];

                for frame in data.chunks_exact_mut(2) {
                    let mut samples = [0.0f32; MAX_CHANNELS];
                    for ch in 0..active {
                        match pb_conss[ch].try_pop() {
                            Some(v) => { any_sample = true; samples[ch] = v; rx_peaks[ch] = rx_peaks[ch].max(v.abs()); }
                            None => { underflowed = true; }
                        }
                    }
                    // Patch matrix routing: output masks determine which received channels
                // go to L and R. Gain is divided by active contributor count per side.
                {
                    let (mut l, mut r) = (0.0, 0.0);
                    let (mut lc, mut rc) = (0u32, 0u32);
                    for ch in 0..active {
                        let bit = 1u64 << ch;
                        let has_sig = gain_snapshot.get(ch).copied().unwrap_or(-120.0) > -80.0
                            || samples[ch].abs() > 0.000_01;
                        if left_mask  & bit != 0 { l += samples[ch]; if has_sig { lc += 1; } }
                        if right_mask & bit != 0 { r += samples[ch]; if has_sig { rc += 1; } }
                    }
                    frame[0] = if lc > 0 { l / lc as f32 } else { l };
                    frame[1] = if rc > 0 { r / rc as f32 } else { r };
                }
                }

                for ch in 0..active { meters_play.set_rx_peak(ch, peak_dbfs_from_peak(rx_peaks[ch])); }

                // Brick-wall limiter — prevents clipping from hot sums or artefacts
                limiter.process(data);

                let mut mpeak = [0.0f32; 2];
                for frame in data.chunks_exact(2) {
                    mpeak[0] = mpeak[0].max(frame[0].abs());
                    mpeak[1] = mpeak[1].max(frame[1].abs());
                }
                meters_play.set_monitor_peak(0, peak_dbfs_from_peak(mpeak[0]));
                meters_play.set_monitor_peak(1, peak_dbfs_from_peak(mpeak[1]));

                if underflowed { underflows_play.fetch_add(1, Ordering::Relaxed); }
                handle_underrun(!any_sample, &started_play, &empty_cb_play);
            },
            move |e| {
                if output_ok_play.swap(false, Ordering::Relaxed) {
                    tracing::error!("Output device lost: {e}. Reconnect device or re-apply settings in the web UI.");
                }
            },
            None,
        )?;
        stream.play()?;
        Some(stream)
    } else {
        tracing::info!("Receive disabled (--no-recv)");
        None
    };

    // ── Send pipeline ──────────────────────────────────────────────────────
    let _in_stream: Option<cpal::Stream> = if send_enabled {
        let socket_tx    = Arc::clone(&socket);
        let connected_tx = Arc::clone(&handshake_connected);
        let ui_stats_tx  = Arc::clone(&ui_stats);
        let meters_tx    = Arc::clone(&meters);
        let tx_masks     = Arc::clone(&tx_tone_source_for_send);

        let in_channels  = local_input_channels.min(MAX_CHANNELS);
        let input_rings: Arc<Mutex<Vec<VecDeque<f32>>>> = Arc::new(Mutex::new(
            (0..in_channels).map(|_| VecDeque::with_capacity(CAP_RING_SIZE)).collect(),
        ));

        let in_stream_opt = if in_channels > 0 {
            match host.default_input_device() {
                Some(_default_dev) => {
                    let in_dev = suppress_alsa_stderr(|| find_input_device(&selected_input_device))
                        .unwrap_or(_default_dev);
                    let (in_cfg, in_device_rate) = suppress_alsa_stderr(|| best_input_config(&in_dev, in_channels));
                    let actual_in_channels = in_cfg.channels as usize;
                    tracing::info!("Input: {} @ {}Hz {}ch{}",
                        in_dev.name()?, in_device_rate, actual_in_channels,
                        if in_device_rate != SAMPLE_RATE { " (resampling to 48kHz)" } else { "" });
                    actual_input_rate.store(in_device_rate, Ordering::Relaxed);
                    // Build a resampler if the device doesn't run at 48kHz
                    let mut in_resampler = make_io_resampler(in_device_rate, SAMPLE_RATE);
                    let rings_cb  = Arc::clone(&input_rings);
                    let meters_cb = Arc::clone(&meters);
                    let mut peak_acc  = vec![0.0f32; actual_in_channels];
                    let mut samp_cnt  = 0usize;
                    // Accumulation buffer: collect mono samples at device rate for resampler
                    let mut resample_accum: Vec<f32> = Vec::with_capacity(
                        (in_device_rate as usize * 25) / 1000);
                    let resample_chunk = (in_device_rate as usize * 20) / 1000;
                    let st = in_dev.build_input_stream(
                        &in_cfg,
                        move |data: &[f32], _| {
                            if let Ok(mut rings) = rings_cb.lock() {
                                for frame in data.chunks(actual_in_channels) {
                                    // Mix down to mono (average all channels)
                                    let s = if actual_in_channels == 1 {
                                        frame[0]
                                    } else {
                                        frame.iter().sum::<f32>() / actual_in_channels as f32
                                    };
                                    // Peak metering (per-channel of actual device)
                                    for ch in 0..actual_in_channels.min(in_channels) {
                                        let v = frame.get(ch).copied().unwrap_or(s);
                                        peak_acc[ch] = peak_acc[ch].max(v.abs());
                                    }
                                    samp_cnt += 1;
                                    if samp_cnt >= FRAME_SAMPLES {
                                        for ch in 0..actual_in_channels.min(in_channels) {
                                            meters_cb.set_input_peak(ch, peak_dbfs_from_peak(peak_acc[ch]));
                                            peak_acc[ch] = 0.0;
                                        }
                                        samp_cnt = 0;
                                    }
                                    if let Some(ref mut rs) = in_resampler {
                                        resample_accum.push(s);
                                        while resample_accum.len() >= resample_chunk {
                                            let chunk: Vec<f32> = resample_accum.drain(..resample_chunk).collect();
                                            if let Ok(out) = rs.process(&[&chunk], None) {
                                                for &v in out[0].iter() {
                                                    // Replicate to all send channels from this device
                                                    for ch in 0..in_channels {
                                                        if let Some(ring) = rings.get_mut(ch) {
                                                            if ring.len() >= CAP_RING_SIZE { ring.pop_front(); }
                                                            ring.push_back(v);
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    } else {
                                        // No resampler needed — push directly
                                        for ch in 0..in_channels {
                                            if let Some(ring) = rings.get_mut(ch) {
                                                if ring.len() >= CAP_RING_SIZE { ring.pop_front(); }
                                                ring.push_back(s);
                                            }
                                        }
                                    }
                                }
                            }
                        },
                        move |e| {
                            if input_ok_err.swap(false, Ordering::Relaxed) {
                                tracing::error!("Input device lost: {e}. Reconnect device or re-apply settings in the web UI.");
                            }
                        },
                        None,
                    )?;
                    st.play()?;
                    Some(st)
                }
                None => { tracing::warn!("No default input device"); None }
            }
        } else { None };

        let input_rings_tx = Arc::clone(&input_rings);
        std::thread::spawn(move || {
            let mut encoders: Vec<opus::Encoder> = (0..num_channels)
                .map(|_| make_encoder_for_mode(opus_bitrate_per_channel, encoder_mode).unwrap())
                .collect();
            tracing::info!("Send: {num_channels} encoder(s) — {} {}kb/s",
                encoder_mode.as_str(), opus_bitrate_per_channel / 1000);
            let mut seqs         = vec![0u16; num_channels];
            let mut ts: u32      = 0;
            let mut abs_sample: u64 = 0;
            let mut frames       = vec![vec![0.0f32; FRAME_SAMPLES]; num_channels];
            let mut compressed   = vec![0u8; 4000];
            let frame_dur        = Duration::from_micros(20_000);
            let mut next_dl      = Instant::now() + frame_dur;
            let mut sent_total:  u64   = 0;
            let mut sent_at:     u64   = 0;
            let mut tx_bytes:    usize = 0;
            let mut tx_bytes_at: usize = 0;
            let mut last_tx_log  = Instant::now();

            loop {
                let connected = connected_tx.load(Ordering::Relaxed);
                let frame_start = abs_sample;
                let mut input_blocks = vec![vec![0.0f32; FRAME_SAMPLES]; in_channels];
                if in_channels > 0 {
                    if let Ok(mut rings) = input_rings_tx.lock() {
                        for ch in 0..in_channels {
                            if let Some(ring) = rings.get_mut(ch) {
                                for i in 0..FRAME_SAMPLES { input_blocks[ch][i] = ring.pop_front().unwrap_or(0.0); }
                            }
                        }
                    }
                }

                for ch in 0..num_channels {
                    let mask = tx_masks.get(ch).map(|v| v.load(Ordering::Relaxed) as u64).unwrap_or(0);
                    let frame = &mut frames[ch];
                    frame.fill(0.0);
                    let mut nsrc = 0usize;
                    for bit in 0..64 {
                        if (mask & (1u64 << bit)) == 0 { continue; }
                        let Some(src) = source_code_from_bit_index(bit) else { continue; };
                        match src {
                            TX_SRC_EBU_L | TX_SRC_EBU_R => {
                                for (i, s) in frame.iter_mut().enumerate() {
                                    *s += tx_source_sample(src, frame_start + i as u64, 0);
                                }
                                nsrc += 1;
                            }
                            code if code >= TX_SRC_INPUT_BASE => {
                                let ich = code - TX_SRC_INPUT_BASE;
                                if let Some(block) = input_blocks.get(ich) {
                                    if block.iter().fold(0.0f32, |m,&v| m.max(v.abs())) > 0.000_001 {
                                        for (d, s) in frame.iter_mut().zip(block.iter()) { *d += *s; }
                                        nsrc += 1;
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                    if nsrc > 1 { let g = 1.0 / nsrc as f32; for s in frame.iter_mut() { *s *= g; } }
                    let peak = frame.iter().fold(0.0f32, |m,&v| m.max(v.abs()));
                    meters_tx.set_tx_peak(ch, peak_dbfs_from_peak(peak));
                    if connected {
                        if let Ok(n) = encoders[ch].encode_float(frame, &mut compressed) {
                            let pkt = build_packet(seqs[ch], ts, ch as u8, &compressed[..n]);
                            tx_bytes = tx_bytes.saturating_add(pkt.len());
                            socket_tx.send(&pkt).ok();
                        }
                        seqs[ch] = seqs[ch].wrapping_add(1);
                    }
                }

                abs_sample = abs_sample.wrapping_add(FRAME_SAMPLES as u64);
                ts = ts.wrapping_add(FRAME_SAMPLES as u32);
                sent_total += 1;

                if last_tx_log.elapsed() >= STATS_LOG_INTERVAL {
                    let elapsed = last_tx_log.elapsed().as_secs_f64().max(0.001);
                    let tx_fps  = (sent_total - sent_at) as f64 / elapsed;
                    sent_at = sent_total;
                    let bd = tx_bytes.saturating_sub(tx_bytes_at);
                    tx_bytes_at = tx_bytes;
                    let tx_mbps = bd as f64 * 8.0 / elapsed / 1_000_000.0;
                    tracing::info!("TX {:.3}Mbps fps={tx_fps:.1} ts={ts}", tx_mbps);
                    if let Ok(mut s) = ui_stats_tx.lock() {
                        s.tx_fps = tx_fps; s.tx_mbps = tx_mbps; s.tx_active_channel = 0;
                        s.tx_peak_dbfs    = meters_tx.snapshot_tx(num_channels);
                        s.input_peak_dbfs = meters_tx.snapshot_input(in_channels);
                    }
                    last_tx_log = Instant::now();
                }

                sleep_until(next_dl);
                next_dl += frame_dur;
                let now = Instant::now();
                while next_dl + frame_dur < now { next_dl += frame_dur; }
            }
        });

        in_stream_opt
    } else {
        tracing::info!("Send disabled (--no-send)");
        None
    };

    tracing::info!("Engine running — Ctrl+C to stop");
    loop { std::thread::sleep(Duration::from_secs(1)); }
}
