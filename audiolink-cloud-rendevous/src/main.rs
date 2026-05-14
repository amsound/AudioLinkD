/// audiolink-cloud — AudioLink rendezvous server
///
/// Deployed behind nginx (TLS termination). Listens on 127.0.0.1:7070.
/// nginx passes the device's real public IP via X-Real-IP header.
///
/// API:
///   POST /api/register         — register / keepalive (10s cadence)
///   POST /api/connect          — request connection to a remote device
///   GET  /api/events/{name}    — long-poll: server pushes connect events here
///   GET  /api/status           — diagnostics: currently registered devices

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Json},
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, Mutex},
    time::timeout,
};
use tracing::info;

// ─── Constants ────────────────────────────────────────────────────────────────

/// A registration expires after this long with no keepalive.
const REGISTRATION_TTL: Duration = Duration::from_secs(30);

/// Long-poll connections are held for up to this long, then the client
/// must reconnect. Kept shorter than REGISTRATION_TTL so a client that
/// stops reconnecting also stops appearing registered.
const LONG_POLL_TIMEOUT: Duration = Duration::from_secs(25);

/// How often the reaper task removes stale registrations.
const REAPER_INTERVAL: Duration = Duration::from_secs(10);

// ─── State ────────────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Registration {
    /// The device's public IP:port for UDP (port is what it registered with,
    /// IP is extracted from X-Real-IP by nginx).
    pub_addr: String,
    last_seen: Instant,
    /// Per-device broadcast channel for pushing connect events to the long-poll.
    /// Capacity 4 — we'll never queue more than a couple of inbound connect
    /// requests simultaneously.
    event_tx: broadcast::Sender<ConnectEvent>,
}

#[derive(Clone, Debug, Serialize)]
struct ConnectEvent {
    /// The device that wants to connect to us.
    from_name: String,
    /// Their public UDP address.
    from_addr: String,
}

type Registry = Arc<Mutex<HashMap<String, Registration>>>;

#[derive(Clone)]
struct AppState {
    registry: Registry,
}

// ─── Request / response types ─────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct RegisterRequest {
    /// The device's own name (matches AudioLink node_id / device_name).
    name: String,
    /// The UDP port the device is listening on (almost always 20102).
    port: u16,
}

#[derive(Debug, Serialize)]
struct RegisterResponse {
    status: &'static str,
    pub_addr: String,
}

#[derive(Debug, Deserialize)]
struct ConnectRequest {
    my_name: String,
    remote_name: String,
}

#[derive(Debug, Serialize)]
struct ConnectResponse {
    /// Public UDP address of the remote device.
    remote_addr: String,
    /// Whether the remote was online and has been notified.
    notified: bool,
}

#[derive(Debug, Serialize)]
struct StatusDevice {
    name: String,
    pub_addr: String,
    last_seen_secs: u64,
}

// ─── Handlers ─────────────────────────────────────────────────────────────────

/// POST /api/register
///
/// Body: { "name": "debian-vm-1", "port": 20102 }
///
/// The device's public IP is taken from X-Real-IP (set by nginx).
/// Called on startup and every 10 seconds as a keepalive.
async fn register_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<RegisterRequest>,
) -> impl IntoResponse {
    let name = req.name.trim().to_string();
    if name.is_empty() || name.len() > 64 {
        return (StatusCode::BAD_REQUEST, "name must be 1–64 characters").into_response();
    }

    // nginx passes the real client IP in X-Real-IP.
    let real_ip = headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    let pub_addr = format!("{real_ip}:{}", req.port);

    let mut registry = state.registry.lock().await;

    let entry = registry.entry(name.clone()).or_insert_with(|| {
        let (tx, _) = broadcast::channel(4);
        Registration {
            pub_addr: pub_addr.clone(),
            last_seen: Instant::now(),
            event_tx: tx,
        }
    });

    entry.pub_addr = pub_addr.clone();
    entry.last_seen = Instant::now();

    info!("register: {name} @ {pub_addr}");

    Json(RegisterResponse {
        status: "ok",
        pub_addr,
    })
    .into_response()
}

/// POST /api/connect
///
/// Body: { "my_name": "debian-vm-1", "remote_name": "debian-vm-2" }
///
/// Returns the remote's current public UDP address so both sides can begin
/// firing 09 07 probes simultaneously (NAT hole punching).
/// Also notifies the remote via its long-poll connection.
async fn connect_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(req): Json<ConnectRequest>,
) -> impl IntoResponse {
    let my_name = req.my_name.trim().to_string();
    let remote_name = req.remote_name.trim().to_string();

    if my_name.is_empty() || remote_name.is_empty() {
        return (StatusCode::BAD_REQUEST, "my_name and remote_name required").into_response();
    }

    let real_ip = headers
        .get("x-real-ip")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("unknown")
        .to_string();

    let registry = state.registry.lock().await;

    let remote = match registry.get(&remote_name) {
        Some(r) if r.last_seen.elapsed() < REGISTRATION_TTL => r,
        _ => {
            return (
                StatusCode::NOT_FOUND,
                format!("remote device '{remote_name}' is not currently registered"),
            )
                .into_response();
        }
    };

    let remote_addr = remote.pub_addr.clone();

    // Work out caller's port from their own registration if present;
    // fall back to 20102 (the standard AudioLink port).
    let my_port = registry
        .get(&my_name)
        .map(|r| r.pub_addr.split(':').last().unwrap_or("20102").to_string())
        .unwrap_or_else(|| "20102".to_string());

    let my_pub_addr = format!("{real_ip}:{my_port}");

    // Notify the remote device via its long-poll channel.
    let notified = remote
        .event_tx
        .send(ConnectEvent {
            from_name: my_name.clone(),
            from_addr: my_pub_addr.clone(),
        })
        .is_ok();

    info!(
        "connect: {my_name} ({my_pub_addr}) → {remote_name} ({remote_addr}) notified={notified}"
    );

    Json(ConnectResponse {
        remote_addr,
        notified,
    })
    .into_response()
}

/// GET /api/events/{name}
///
/// Long-poll: holds the connection open for up to LONG_POLL_TIMEOUT seconds.
/// Returns a JSON connect event when another device requests a connection,
/// or 204 No Content on timeout (client should reconnect immediately).
///
/// The device must be registered before subscribing to events.
async fn events_handler(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let event_rx = {
        let registry = state.registry.lock().await;
        match registry.get(&name) {
            Some(r) => r.event_tx.subscribe(),
            None => {
                return (
                    StatusCode::NOT_FOUND,
                    format!("device '{name}' is not registered — register first"),
                )
                    .into_response();
            }
        }
    };

    // Wait for an event or timeout.
    match timeout(LONG_POLL_TIMEOUT, wait_for_event(event_rx)).await {
        Ok(Ok(event)) => Json(event).into_response(),
        // Timeout or channel closed — client reconnects.
        _ => StatusCode::NO_CONTENT.into_response(),
    }
}

async fn wait_for_event(
    mut rx: broadcast::Receiver<ConnectEvent>,
) -> Result<ConnectEvent, broadcast::error::RecvError> {
    rx.recv().await
}

/// GET /api/status
///
/// Returns currently registered devices and their last-seen age.
/// Useful for diagnostics — not sensitive since device names and IPs
/// are visible here. Restrict with nginx auth if desired.
async fn status_handler(State(state): State<AppState>) -> impl IntoResponse {
    let registry = state.registry.lock().await;
    let devices: Vec<StatusDevice> = registry
        .iter()
        .filter(|(_, r)| r.last_seen.elapsed() < REGISTRATION_TTL)
        .map(|(name, r)| StatusDevice {
            name: name.clone(),
            pub_addr: r.pub_addr.clone(),
            last_seen_secs: r.last_seen.elapsed().as_secs(),
        })
        .collect();
    Json(devices)
}

// ─── Reaper task ──────────────────────────────────────────────────────────────

/// Background task: removes registrations older than REGISTRATION_TTL.
async fn reaper(registry: Registry) {
    loop {
        tokio::time::sleep(REAPER_INTERVAL).await;
        let mut reg = registry.lock().await;
        let before = reg.len();
        reg.retain(|_, r| r.last_seen.elapsed() < REGISTRATION_TTL);
        let removed = before - reg.len();
        if removed > 0 {
            info!("reaper: removed {removed} expired registration(s), {} active", reg.len());
        }
    }
}

// ─── Main ─────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    let registry: Registry = Arc::new(Mutex::new(HashMap::new()));

    // Spawn the reaper.
    tokio::spawn(reaper(Arc::clone(&registry)));

    let state = AppState { registry };

    let app = Router::new()
        .route("/api/register", post(register_handler))
        .route("/api/connect", post(connect_handler))
        .route("/api/events/:name", get(events_handler))
        .route("/api/status", get(status_handler))
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], 7070));
    info!("audiolink-cloud listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}
