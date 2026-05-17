mod constants;
mod packet;
mod state;
mod persistence;
mod routing;
mod audio;
mod jitter;
mod interfaces;
mod web;
mod engine;

use anyhow::Result;

use constants::*;
use state::{EncoderMode, JitterConfig, MonitorMode};
use packet::{
    default_node_id, derive_link_token, derive_token_from_text,
    parse_token_arg, token_to_hex,
};
use persistence::load_persisted_state;
use jitter::effective_receive_buffer_ms;
use engine::run_bidir;
use web::{spawn_web_ui, control_web_state};

/// Lightweight rendezvous registration — runs even in web-first mode so the
/// node is discoverable before the audio engine starts.
fn spawn_rendezvous_registration(rdv_url: String, node_id: String) {
    std::thread::spawn(move || {
        let base = {
            let u = rdv_url.trim_end_matches('/');
            if u.starts_with("http://") || u.starts_with("https://") { u.to_string() }
            else { format!("https://{u}") }
        };
        let body = serde_json::json!({ "name": node_id, "port": PORT }).to_string();
        loop {
            match ureq::post(&format!("{base}/api/register"))
                .set("Content-Type", "application/json")
                .send_string(&body)
            {
                Ok(_)  => tracing::debug!("rendezvous: registered '{node_id}'"),
                Err(e) => tracing::warn!("rendezvous: register failed: {e}"),
            }
            std::thread::sleep(std::time::Duration::from_secs(10));
        }
    });
}

fn main() -> Result<()> {
    // Logging: RUST_LOG overrides; default is "info"
    // RUST_LOG controls log level; default is info.
    // Add features = ["env-filter"] to tracing-subscriber in Cargo.toml for RUST_LOG support.
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    tracing::info!("AudioLink starting");

    let args: Vec<String> = std::env::args().collect();
    let mode = args.get(1).map(String::as_str).unwrap_or("bidir");

    match mode {
        "bidir" | "bidirectional" => run_bidir_from_args(&args),
        "help"  | "--help" | "-h" => { print_usage(); Ok(()) }
        // Legacy modes removed — use bidir
        "send" | "recv" | "echo" => {
            eprintln!("Legacy mode '{mode}' removed — use 'bidir' instead");
            print_usage(); std::process::exit(1);
        }
        _ => { eprintln!("Unknown mode '{mode}'"); print_usage(); std::process::exit(1); }
    }
}

// ─── bidir ───────────────────────────────────────────────────────────────────

fn run_bidir_from_args(args: &[String]) -> Result<()> {
    let saved = load_persisted_state().config;

    // Parse args, with persisted values as fallback
    let mut remote_host = String::new();
    let mut remote_device_name = saved.remote_device_name.clone().unwrap_or_default();
    let mut node_id  = saved.node_id.clone().unwrap_or_else(default_node_id);
    let mut channels: usize = saved.channels.unwrap_or(2);
    let mut bitrate: u32 = saved.opus_bitrate_per_channel.unwrap_or(128_000);
    let mut latency_ms: u32 = saved.latency_ms.unwrap_or(100);
    let mut fixed_jitter = saved.fixed_jitter.unwrap_or(false);
    let mut phase_lock   = saved.phase_lock.unwrap_or(true);
    let mut web_port: Option<u16> = Some(8080);
    let mut no_web  = false;
    let mut no_send = false;
    let mut no_recv = false;
    let mut explicit_token: Option<[u8; 16]> = saved.token_hex.as_deref()
        .and_then(|h| parse_token_arg(h).ok());
    let mut link_password: Option<String> = saved.link_password.clone();
    let mut rendezvous_url: Option<String> = saved.rendezvous_url.clone();
    let mut encoder_mode: EncoderMode = saved.encoder_mode.unwrap_or_default();
    let mut bind_addr: String = saved.bind_addr.clone().unwrap_or_else(|| "0.0.0.0".to_string());
    let mut selected_input_device: Option<String> = saved.selected_input_device.clone();
    let mut selected_output_device: Option<String> = saved.selected_output_device.clone();

    let mut i = 2;
    while i < args.len() {
        match args[i].as_str() {
            "--remote-host" | "-r" => {
                i += 1; remote_host = args.get(i).cloned().unwrap_or_default();
            }
            "--remote-name" => { i += 1; remote_device_name = args.get(i).cloned().unwrap_or_default(); }
            "--id"          => { i += 1; node_id = args.get(i).cloned().unwrap_or_else(default_node_id); }
            "--channels" | "-c" => {
                i += 1;
                channels = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(channels);
            }
            "--bitrate" | "-b" => {
                i += 1;
                bitrate = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(bitrate);
            }
            "--latency-ms" | "--buffer" => {
                i += 1;
                latency_ms = args.get(i).and_then(|s| s.parse().ok()).unwrap_or(latency_ms);
            }
            "--token" => {
                i += 1;
                if let Some(s) = args.get(i) { explicit_token = parse_token_arg(s).ok(); }
            }
            "--link-password" => { i += 1; link_password = args.get(i).cloned(); }

            "--web-port" => {
                i += 1;
                web_port = args.get(i).and_then(|s| s.parse().ok());
            }
            "--no-web"         => { no_web = true; }
            "--no-send"        => { no_send = true; }
            "--no-recv"        => { no_recv = true; }
            "--fixed-jitter"   => { fixed_jitter = true; }
            "--no-phase-lock"  => { phase_lock = false; }
            "--rendezvous"     => { i += 1; rendezvous_url = args.get(i).cloned(); }
            "--encoder-mode"   => {
                i += 1;
                if let Some(s) = args.get(i) { encoder_mode = EncoderMode::parse(s); }
            }
            "--interface"      => { i += 1; bind_addr = args.get(i).cloned().unwrap_or(bind_addr); }
            "--input-device"   => { i += 1; selected_input_device = args.get(i).cloned(); }
            "--output-device"  => { i += 1; selected_output_device = args.get(i).cloned(); }
            arg if !arg.starts_with('-') && i == 2 => {
                // Positional: first non-flag arg can be remote host
                remote_host = arg.to_string();
            }
            arg => { tracing::warn!("Unknown arg: {arg}"); }
        }
        i += 1;
    }

    channels = channels.clamp(1, MAX_CHANNELS);
    let effective_latency_ms = effective_receive_buffer_ms(latency_ms);

    let shared_token: [u8; 16] = if let Some(t) = explicit_token {
        t
    } else if !remote_device_name.is_empty() {
        derive_link_token(
            &node_id, &remote_device_name,
            link_password.as_deref().filter(|p| !p.is_empty()),
        )
    } else {
        DEFAULT_SHARED_TOKEN
    };
    let device_name_token = derive_token_from_text(&node_id);

    tracing::info!(
        "bidir: id={node_id} remote_device={remote_device_name} remote_host={} channels={channels} \
         bitrate={bitrate} latency={latency_ms}ms effective={effective_latency_ms}ms token={}",
        if remote_host.is_empty() { "<none>" } else { &remote_host },
        token_to_hex(&shared_token)
    );

    let jitter = JitterConfig {
        configured_delay_ms: latency_ms,
        target_delay_ms: effective_latency_ms,
        adaptive: !fixed_jitter,
        phase_lock,
    };

    // Web-first mode: only enter if no remote is configured at all.
    // If remote_device_name is set (even without an IP), go into run_bidir
    // so the rendezvous thread starts and the node registers as connectable.
    if remote_host.is_empty() && remote_device_name.is_empty() && !no_web {
        // Register with rendezvous even in web-first mode so the node is
        // discoverable while the user is configuring it.
        if let Some(ref url) = rendezvous_url {
            if !url.trim().is_empty() {
                spawn_rendezvous_registration(url.clone(), node_id.clone());
                tracing::info!("rendezvous: background registration started for '{node_id}'");
            }
        }
        let state = control_web_state(
            node_id, shared_token, channels, bitrate, jitter, encoder_mode,
            rendezvous_url.clone(),
        )?;
        let web_addr = format!("0.0.0.0:{}", web_port.unwrap_or(8080));
        spawn_web_ui(web_addr, state);
        tracing::info!("Web-first mode: no remote host — configure via web UI");
        loop { std::thread::sleep(std::time::Duration::from_secs(1)); }
    }

    let web_addr = if no_web { None } else {
        Some(format!("0.0.0.0:{}", web_port.unwrap_or(8080)))
    };

    run_bidir(
        &remote_host, &remote_device_name, channels,
        !no_send, !no_recv, device_name_token, shared_token, node_id, jitter,
        web_addr, bitrate, rendezvous_url, encoder_mode, bind_addr,
        selected_input_device, selected_output_device,
    )
}

// ─── Legacy modes ────────────────────────────────────────────────────────────

fn print_usage() {
    println!(
        r#"audiolinkd — broadcast audio over IP

USAGE:
  audiolinkd bidir [OPTIONS]             Bidirectional audio (default)
  audiolinkd send  <host> [ch] [bps]     Legacy send-only
  audiolinkd recv  [ch] [latency_ms]     Legacy receive-only
  audiolinkd echo                        UDP echo test

BIDIR OPTIONS:
  --remote-host <host>      Remote IP or hostname
  --remote-name <name>      Remote device name (for token derivation)
  --id <name>               Local device name [{default}]
  --channels <n>            Network send channels [2]
  --bitrate <bps>           Opus bitrate per channel [128000]
  --latency-ms <ms>         Receive buffer size [100]
  --token <hex|text>        Explicit shared token
  --link-password <pw>      Shared link password
  --monitor <mode>          compat | matrix [compat]
  --encoder-mode <mode>     music | speech [music]
  --interface <ip>          Bind to specific IP [0.0.0.0]
  --rendezvous <url>        Rendezvous server URL
  --web-port <port>         Web UI port [8080]
  --no-web                  Disable web UI
  --no-send                 Disable send
  --no-recv                 Disable receive
  --fixed-jitter            Disable ASRC drift correction
  --no-phase-lock           Disable phase-locked frame grouping

Configuration is persisted to audiolinkd_config.json.
"#,
        default = default_node_id()
    );
}
