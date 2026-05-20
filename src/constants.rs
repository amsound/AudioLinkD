use std::time::Duration;

pub const SAMPLE_RATE: u32 = 48_000;
pub const FRAME_SAMPLES: usize = 960;       // 20 ms at 48 kHz
pub const FRAME_MS: u32 = 20;
pub const MAX_CHANNELS: usize = 64;
pub const PORT: u16 = 20102;

pub const SSRC_CONTROL: u32 = 0x0000_0200;
pub const SSRC_METADATA: u32 = 0x0000_0000;
pub const SSRC_MEDIA: u32 = 0x0001_0000;

pub const DEFAULT_SHARED_TOKEN: [u8; 16] = [0xa5; 16];

pub const PRIME_SAMPLES: usize = 960 * 6;  // 120 ms
pub const MIN_LATENCY_MS: u32 = 5;
pub const MAX_LATENCY_MS: u32 = 10_000;
pub const MIN_EFFECTIVE_RX_BUFFER_MS: u32 = FRAME_MS * 2;
pub const PHASE_LOCK_TIMEOUT_MS: u64 = 10;

pub const RTP_RESTART_ROLLBACK_SAMPLES: u32 = SAMPLE_RATE;
pub const RTP_RESTART_LOW_TS_SAMPLES: u32 = SAMPLE_RATE * 2;

pub const STATS_LOG_INTERVAL: Duration = Duration::from_secs(5);
pub const INTERFACE_POLL_INTERVAL: Duration = Duration::from_secs(3);

pub const CAP_RING_SIZE: usize = (SAMPLE_RATE as usize * 200) / 1000;
pub const PB_RING_SIZE: usize = SAMPLE_RATE as usize;

pub const REPRIME_AFTER_EMPTY_CALLBACKS: u32 = 30;
pub const CROSSFADE_SAMPLES: usize = 64;

// EBU R49 line-up tone: 1 kHz at -18 dBFS peak.
pub const TONE_AMPLITUDE: f32 = 0.125_892_54;

pub const TX_SRC_EBU_L: usize = 100;
pub const TX_SRC_EBU_R: usize = 101;
pub const TX_SRC_INPUT_BASE: usize = 1000;
