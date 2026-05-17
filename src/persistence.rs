use serde::{Deserialize, Serialize};
use crate::state::{EncoderMode, Route};
use crate::packet::local_channel_label;

// ─── Persisted types ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PersistedRuntimeConfig {
    pub remote: Option<String>,
    pub remote_device_name: Option<String>,
    pub link_password: Option<String>,
    pub node_id: Option<String>,
    pub token_hex: Option<String>,
    pub channels: Option<usize>,
    pub opus_bitrate_per_channel: Option<u32>,
    pub latency_ms: Option<u32>,
    pub fixed_jitter: Option<bool>,
    pub phase_lock: Option<bool>,
    pub encoder_mode: Option<EncoderMode>,
    pub channel_labels: Option<Vec<String>>,
    pub rendezvous_url: Option<String>,
    pub bind_addr: Option<String>,
    pub selected_input_device: Option<String>,
    pub selected_output_device: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PersistedUiState {
    #[serde(default)]
    pub config: PersistedRuntimeConfig,
    #[serde(default)]
    pub routes: Vec<Route>,
}

// ─── File paths ──────────────────────────────────────────────────────────────

pub fn persisted_state_path() -> std::path::PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join("audiolinkd_config.json")
}

pub fn legacy_persisted_state_path() -> std::path::PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| std::path::PathBuf::from("."))
        .join(".audiolinkd_m7_state.json")
}

// ─── Load / save ─────────────────────────────────────────────────────────────

pub fn load_persisted_state() -> PersistedUiState {
    for path in [persisted_state_path(), legacy_persisted_state_path()] {
        let Ok(text) = std::fs::read_to_string(&path) else { continue; };
        match serde_json::from_str::<PersistedUiState>(&text) {
            Ok(state) => return state,
            Err(e) => tracing::warn!(
                "Ignoring unreadable AudioLink config at {}: {e}",
                path.display()
            ),
        }
    }
    PersistedUiState::default()
}

pub fn save_persisted_state(state: &PersistedUiState) {
    match serde_json::to_string_pretty(state) {
        Ok(text) => {
            if let Err(e) = std::fs::write(persisted_state_path(), text) {
                tracing::warn!("Could not save AudioLink config: {e}");
            }
        }
        Err(e) => tracing::warn!("Could not serialise AudioLink config: {e}"),
    }
}

pub fn save_persisted_routes(routes: &[Route]) {
    let mut state = load_persisted_state();
    state.routes = routes.to_vec();
    save_persisted_state(&state);
}

pub fn save_persisted_config(config: PersistedRuntimeConfig) {
    let mut state = load_persisted_state();
    let preserved_labels = state.config.channel_labels.clone();
    state.config = config;
    state.config.channel_labels = preserved_labels;
    save_persisted_state(&state);
}

pub fn load_persisted_labels(num_channels: usize) -> Vec<String> {
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
