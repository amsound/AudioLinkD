use serde::{Deserialize, Serialize};
use crate::state::{EncoderMode, Route};
use crate::packet::local_channel_label;

// ─── RemoteConnection ─────────────────────────────────────────────────────────

/// A single named remote peer. Replaces the flat single-remote fields that
/// previously lived directly on PersistedRuntimeConfig.
///
/// `index` is stable — it is used as the routing namespace in endpoint IDs:
///   send:  "stream:{index}:ch:N"
///   recv:  "peer:{index}:ch:N"
/// It is never reassigned after creation, even if other entries are removed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteConnection {
    /// Routing namespace index. Stable for the lifetime of this entry.
    pub index: usize,
    /// Must match the remote node's node_id for token derivation.
    pub name: String,
    /// Remote IP/host. Blank = cloud rendezvous.
    #[serde(default)]
    pub ip_override: String,
    /// Per-remote link password. None falls back to DEFAULT_SHARED_TOKEN path.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Which local send channels (0..63) to transmit to this remote.
    #[serde(default)]
    pub channels_to_send: Vec<usize>,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool { true }

// ─── Persisted types ──────────────────────────────────────────────────────────

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct PersistedRuntimeConfig {
    // ── Remote connections ───────────────────────────────────────────────────
    #[serde(default)]
    pub remotes: Vec<RemoteConnection>,

    // ── Unchanged fields ─────────────────────────────────────────────────────
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
            Ok(state) => {
                return state;
            }
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

/// Save codec/transport config, preserving fields that arrive via separate
/// mechanisms and must not be wiped by a SetupApplyRequest:
///
///   channel_labels — saved via /api/local-labels, not setup/apply
///   remotes        — saved via /api/remotes (M13), not setup/apply
///
/// SetupApplyRequest sends `remotes: vec![]` because it has no remotes
/// payload. We only overwrite the stored list if the incoming config
/// explicitly carries entries.
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

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // T09 — new format serialises without legacy fields, and round-trips cleanly
    #[test]
    fn t09_new_format_roundtrips_without_legacy_fields() {
        let state = PersistedUiState {
            config: PersistedRuntimeConfig {
                remotes: vec![RemoteConnection {
                    index: 0,
                    name: "StudioB".to_string(),
                    ip_override: "192.168.1.50".to_string(),
                    password: Some("secret".to_string()),
                    channels_to_send: vec![0, 1, 2, 3],
                    enabled: true,
                }],
                channels: Some(4),
                ..Default::default()
            },
            routes: vec![],
        };
        let json = serde_json::to_string_pretty(&state).expect("serialise failed");
        assert!(!json.contains("\"remote_device_name\""),
            "legacy remote_device_name should not appear in output");
        assert!(!json.contains("\"link_password\""),
            "legacy link_password should not appear in output");
        let state2: PersistedUiState =
            serde_json::from_str(&json).expect("re-deserialise failed");
        assert_eq!(state2.config.remotes.len(), 1);
        assert_eq!(state2.config.remotes[0].name, "StudioB");
        assert_eq!(state2.config.remotes[0].password.as_deref(), Some("secret"));
    }

}
