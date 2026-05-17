use std::net::UdpSocket;
use std::sync::{
    atomic::{AtomicBool, AtomicI32, AtomicU8, AtomicU32, AtomicU64, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Instant;
use std::collections::HashMap;
use serde::{Deserialize, Serialize};
use crate::constants::MAX_CHANNELS;
use crate::packet::{now_millis, RemoteMetadata};

// ─── EncoderMode ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum EncoderMode {
    #[default]
    Music,
    Speech,
}

impl EncoderMode {
    pub fn as_str(self) -> &'static str {
        match self { Self::Music => "music", Self::Speech => "speech" }
    }
    pub fn parse(s: &str) -> Self {
        match s.trim().to_lowercase().as_str() {
            "speech" | "voice" | "voip" => Self::Speech,
            _ => Self::Music,
        }
    }
}

// ─── JitterConfig ────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct JitterConfig {
    pub configured_delay_ms: u32,
    pub target_delay_ms: u32,
    pub adaptive: bool,
    pub phase_lock: bool,
}

// ─── MonitorMode ─────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MonitorMode {
    PatchMatrix,
}

impl MonitorMode {
    pub fn as_u8(self) -> u8 { 0 }
    pub fn from_u8(_value: u8) -> Self { Self::PatchMatrix }
}

// ─── PeerStatus ──────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum PeerStatus { Gray, Green, Orange }

// ─── Route / Endpoint / MatrixResponse ───────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Route {
    pub source: String,
    pub destination: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Endpoint {
    pub id: String,
    pub label: String,
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MatrixResponse {
    pub monitor_mode: MonitorMode,
    pub sources: Vec<Endpoint>,
    pub destinations: Vec<Endpoint>,
    pub routes: Vec<Route>,
}

// ─── UiStats ─────────────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct UiStats {
    pub channels: usize,
    pub fill_ms: usize,
    pub target_ms: u32,
    pub phase_lock: bool,
    pub queued_groups: usize,
    pub decoded_fps: f64,
    pub output_underflows: usize,
    pub plc_channels: usize,
    pub seq_missing: usize,
    pub loss_percent: f64,
    pub jitter_ms: f64,
    pub jitter_p95_ms: f64,
    pub recommended_buffer_ms: u32,
    pub latency_ms: usize,
    pub rx_mbps: f64,
    pub tx_mbps: f64,
    pub drift_pressure_ppm: isize,
    pub tx_fps: f64,
    pub tx_active_channel: usize,
    pub tx_peak_dbfs: Vec<f32>,
    pub input_peak_dbfs: Vec<f32>,
    pub rx_peak_dbfs: Vec<f32>,
    pub monitor_peak_dbfs: [f32; 2],
    pub rtt_ms: f64,
    pub one_way_latency_ms: f64,
    pub ring_overflows: usize,
}

// ─── RuntimeSummary ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize)]
pub struct RuntimeSummary {
    pub mode: String,
    pub remote_host: String,
    pub remote_device_name: String,
    pub source: String,
    pub codec: String,
    pub opus_bitrate_per_channel: u32,
    pub frame_ms: u32,
    pub tx_channels: usize,
    pub token_configured: bool,
    pub token_hint: String,
    #[serde(skip_serializing)]
    pub token_hex: String,
    pub send_enabled: bool,
    pub recv_enabled: bool,
    pub latency_ms: u32,
    pub effective_latency_ms: u32,
    pub fixed_jitter: bool,
    pub phase_lock: bool,
    pub encoder_mode: String,
    pub link_password_configured: bool,
    pub rendezvous_url: String,
    pub bind_addr: String,
    pub selected_input_device: Option<String>,
    pub selected_output_device: Option<String>,
    pub web_note: String,
}

// ─── DeviceResponse ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DeviceResponse {
    pub sample_rate: u32,
    pub default_input: String,
    pub default_output: String,
    pub default_input_channels: usize,
    pub default_output_channels: usize,
    pub default_input_sample_rate: u32,
    pub default_output_sample_rate: u32,
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

// ─── MeterBank ───────────────────────────────────────────────────────────────

pub struct MeterBank {
    pub tx_peak_db_x100: Vec<AtomicI32>,
    pub rx_peak_db_x100: Vec<AtomicI32>,
    pub input_peak_db_x100: Vec<AtomicI32>,
    pub monitor_peak_db_x100: [AtomicI32; 2],
}

impl MeterBank {
    pub fn new() -> Self {
        Self {
            tx_peak_db_x100:    (0..MAX_CHANNELS).map(|_| AtomicI32::new(-12000)).collect(),
            rx_peak_db_x100:    (0..MAX_CHANNELS).map(|_| AtomicI32::new(-12000)).collect(),
            input_peak_db_x100: (0..MAX_CHANNELS).map(|_| AtomicI32::new(-12000)).collect(),
            monitor_peak_db_x100: [AtomicI32::new(-12000), AtomicI32::new(-12000)],
        }
    }
    pub fn set_tx_peak(&self, ch: usize, db: f32) {
        if ch < self.tx_peak_db_x100.len() {
            self.tx_peak_db_x100[ch].store((db * 100.0).round() as i32, Ordering::Relaxed);
        }
    }
    pub fn set_rx_peak(&self, ch: usize, db: f32) {
        if ch < self.rx_peak_db_x100.len() {
            self.rx_peak_db_x100[ch].store((db * 100.0).round() as i32, Ordering::Relaxed);
        }
    }
    pub fn set_input_peak(&self, ch: usize, db: f32) {
        if ch < self.input_peak_db_x100.len() {
            self.input_peak_db_x100[ch].store((db * 100.0).round() as i32, Ordering::Relaxed);
        }
    }
    pub fn set_monitor_peak(&self, side: usize, db: f32) {
        if side < 2 {
            self.monitor_peak_db_x100[side].store((db * 100.0).round() as i32, Ordering::Relaxed);
        }
    }
    pub fn snapshot_tx(&self, n: usize) -> Vec<f32> {
        self.tx_peak_db_x100.iter().take(n.min(MAX_CHANNELS)).map(|v| v.load(Ordering::Relaxed) as f32 / 100.0).collect()
    }
    pub fn snapshot_rx(&self, n: usize) -> Vec<f32> {
        self.rx_peak_db_x100.iter().take(n.min(MAX_CHANNELS)).map(|v| v.load(Ordering::Relaxed) as f32 / 100.0).collect()
    }
    pub fn snapshot_input(&self, n: usize) -> Vec<f32> {
        self.input_peak_db_x100.iter().take(n.min(MAX_CHANNELS)).map(|v| v.load(Ordering::Relaxed) as f32 / 100.0).collect()
    }
    pub fn snapshot_monitor(&self) -> [f32; 2] {
        [
            self.monitor_peak_db_x100[0].load(Ordering::Relaxed) as f32 / 100.0,
            self.monitor_peak_db_x100[1].load(Ordering::Relaxed) as f32 / 100.0,
        ]
    }
}

// ─── WebState ────────────────────────────────────────────────────────────────

#[derive(Clone)]
pub struct WebState {
    pub started_at: Instant,
    pub node_id: String,
    pub local_channels: usize,
    pub local_input_channels: usize,
    pub send_enabled: bool,
    pub recv_enabled: bool,
    pub handshake_connected: Arc<AtomicBool>,
    pub last_control_ms: Arc<AtomicU64>,
    pub last_audio_ms: Arc<AtomicU64>,
    pub remote_channels: Arc<AtomicUsize>,
    pub remote_metadata: Arc<Mutex<Option<RemoteMetadata>>>,
    pub monitor_mode: Arc<AtomicU8>,
    pub output_route_masks: Arc<[AtomicU64; 2]>,
    pub tx_tone_source_for_send: Arc<Vec<AtomicUsize>>,
    pub local_labels: Arc<Mutex<Vec<String>>>,
    pub metadata_socket: Arc<UdpSocket>,
    pub device_name_token: [u8; 16],
    pub routes: Arc<Mutex<Vec<Route>>>,
    pub presets: Arc<Mutex<HashMap<String, Vec<Route>>>>,
    pub stats: Arc<Mutex<UiStats>>,
    pub meters: Arc<MeterBank>,
    pub runtime: RuntimeSummary,
    pub devices: Arc<DeviceResponse>,
    pub restart_lock: Arc<Mutex<()>>,
    pub established_peer_addr: Arc<Mutex<Option<std::net::SocketAddr>>>,
    pub rtt_us10: Arc<AtomicU32>,
    pub actual_input_rate: Arc<AtomicU32>,
    pub actual_output_rate: Arc<AtomicU32>,
    pub remote_conflict: Arc<Mutex<Option<String>>>,
}

impl WebState {
    pub fn peer_status(&self) -> PeerStatus {
        let now_ms = now_millis();
        let last_control = self.last_control_ms.load(Ordering::Relaxed);
        let last_audio   = self.last_audio_ms.load(Ordering::Relaxed);
        let control_age_ms = now_ms.saturating_sub(last_control);
        let audio_age_ms   = now_ms.saturating_sub(last_audio);

        if !self.handshake_connected.load(Ordering::Relaxed) || last_control == 0 {
            return PeerStatus::Gray;
        }
        if control_age_ms > 5_000 { return PeerStatus::Gray; }
        if self.recv_enabled
            && ((last_audio == 0 && control_age_ms > 1_000)
                || (last_audio > 0 && audio_age_ms > 1_000))
        {
            return PeerStatus::Orange;
        }
        if let Ok(stats) = self.stats.lock() {
            if stats.output_underflows > 0 || stats.seq_missing > 0 {
                return PeerStatus::Orange;
            }
        }
        PeerStatus::Green
    }

    pub fn monitor_mode(&self) -> MonitorMode {
        MonitorMode::from_u8(self.monitor_mode.load(Ordering::Relaxed))
    }
}
