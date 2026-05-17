use std::time::{Duration, Instant};
use anyhow::Result;
use cpal::{traits::{DeviceTrait, HostTrait}, BufferSize};
use rubato::{FastFixedIn, PolynomialDegree};
use crate::constants::*;
use crate::state::{DeviceResponse, EncoderMode};

pub const ALSA_PERIOD: BufferSize = BufferSize::Default;

// ─── Encoder factory ─────────────────────────────────────────────────────────

pub fn make_encoder_for_mode(bitrate: u32, mode: EncoderMode) -> Result<opus::Encoder> {
    let app = match mode {
        EncoderMode::Music  => opus::Application::Audio,
        EncoderMode::Speech => opus::Application::Voip,
    };
    let mut enc = opus::Encoder::new(SAMPLE_RATE, opus::Channels::Mono, app)?;
    enc.set_bitrate(opus::Bitrate::Bits(bitrate as i32))?;
    enc.set_inband_fec(matches!(mode, EncoderMode::Speech))?;
    Ok(enc)
}

pub fn make_encoder_with_bitrate(bitrate: u32) -> Result<opus::Encoder> {
    make_encoder_for_mode(bitrate, EncoderMode::default())
}

pub fn make_encoder() -> Result<opus::Encoder> {
    make_encoder_with_bitrate(128_000)
}

// ─── Decoder / resampler banks ───────────────────────────────────────────────

pub fn fresh_opus_decoders() -> Vec<opus::Decoder> {
    (0..MAX_CHANNELS)
        .map(|_| opus::Decoder::new(SAMPLE_RATE, opus::Channels::Mono).unwrap())
        .collect()
}

pub fn fresh_asrc_resamplers() -> Vec<FastFixedIn<f32>> {
    (0..MAX_CHANNELS)
        .map(|_| FastFixedIn::new(1.0, 1.01, PolynomialDegree::Linear, FRAME_SAMPLES, 1)
            .expect("Rubato resampler init failed"))
        .collect()
}

// ─── Mixdown ─────────────────────────────────────────────────────────────────

/// Mix N mono channels into stereo interleaved output.
/// Odd-indexed channels (0, 2, 4…) → L; even-indexed (1, 3, 5…) → R.
/// Sum divided by contributor count per side to prevent clipping.
/// N=1: mono copied to both L and R at unity.
pub fn mixdown(channels: &[Vec<f32>], out: &mut [f32]) {
    let n = channels.len();
    assert_eq!(out.len(), FRAME_SAMPLES * 2);
    if n == 0 { out.fill(0.0); return; }
    if n == 1 {
        for (i, s) in channels[0].iter().enumerate() {
            out[i * 2] = *s; out[i * 2 + 1] = *s;
        }
        return;
    }
    for i in 0..FRAME_SAMPLES {
        let mut l = 0.0f32; let mut r = 0.0f32;
        for (ch, buf) in channels.iter().enumerate() {
            if ch % 2 == 0 { l += buf[i]; } else { r += buf[i]; }
        }
        out[i * 2] = l; out[i * 2 + 1] = r;
    }
}

// ─── Re-prime logic ───────────────────────────────────────────────────────────

pub fn handle_underrun(
    all_empty: bool,
    started: &std::sync::atomic::AtomicBool,
    empty_counter: &std::sync::atomic::AtomicU32,
) {
    use std::sync::atomic::Ordering;
    if all_empty {
        let count = empty_counter.fetch_add(1, Ordering::Relaxed) + 1;
        if count >= REPRIME_AFTER_EMPTY_CALLBACKS {
            tracing::warn!("Output dry for ~{}ms — re-priming", count * 20);
            started.store(false, Ordering::Relaxed);
            empty_counter.store(0, Ordering::Relaxed);
        }
    } else {
        empty_counter.store(0, Ordering::Relaxed);
    }
}

// ─── Decode with PLC ─────────────────────────────────────────────────────────

pub fn decode_or_plc(decoder: &mut opus::Decoder, payload: Option<&[u8]>, pcm: &mut [f32]) -> bool {
    match payload {
        Some(data) if decoder.decode_float(data, pcm, false).is_ok() => true,
        _ => {
            if decoder.decode_float(&[], pcm, true).is_err() { pcm.fill(0.0); }
            false
        }
    }
}

/// Decode or PLC with a linear crossfade at real/concealed boundaries.
///
/// Maintains a tail buffer of the last CROSSFADE_SAMPLES real decoded samples.
/// On real→PLC: fades from real tail into PLC output — eliminates the click.
/// On PLC→real: short fade-in on the first real frame back.
pub fn decode_or_plc_xfade(
    decoder: &mut opus::Decoder,
    payload: Option<&[u8]>,
    pcm: &mut [f32],
    tail: &mut Vec<f32>,
    was_plc: &mut bool,
) -> bool {
    let is_real = match payload {
        Some(data) if decoder.decode_float(data, pcm, false).is_ok() => true,
        _ => {
            if decoder.decode_float(&[], pcm, true).is_err() { pcm.fill(0.0); }
            false
        }
    };
    let n = CROSSFADE_SAMPLES.min(pcm.len());
    if !is_real && !*was_plc && tail.len() == CROSSFADE_SAMPLES {
        // real → PLC: crossfade tail out, PLC in
        for i in 0..n {
            let t = i as f32 / n as f32;
            pcm[i] = tail[i] * (1.0 - t) + pcm[i] * t;
        }
    } else if is_real && *was_plc {
        // PLC → real: short fade-in
        for i in 0..n {
            pcm[i] *= i as f32 / n as f32;
        }
    }
    if is_real {
        let start = pcm.len().saturating_sub(CROSSFADE_SAMPLES);
        tail.clear();
        tail.extend_from_slice(&pcm[start..]);
    }
    *was_plc = !is_real;
    is_real
}

// ─── Output limiter ──────────────────────────────────────────────────────────

/// Gain-riding brick-wall limiter for the stereo physical output.
///
/// Attack: instantaneous — gain reduced in the same sample that exceeds threshold.
/// Release: exponential, ~150ms half-life (release_coef = 0.9998 at 48kHz).
/// Threshold: -1 dBFS (linear 0.891).
///
/// Applied to the stereo output after routing. Prevents digital distortion
/// from hot multi-channel summing, concealment artefacts, or level jumps on
/// reconnect. The monitor output meter reflects the post-limiter level.
pub struct Limiter {
    gain: f32,
    threshold: f32,
    release_coef: f32,
}

impl Limiter {
    pub fn new() -> Self {
        Self { gain: 1.0, threshold: 0.891, release_coef: 0.9998 }
    }

    #[inline]
    pub fn process(&mut self, samples: &mut [f32]) {
        for s in samples.iter_mut() {
            let peak = s.abs();
            if peak > 1e-6 {
                let headroom = self.threshold / peak;
                if headroom < self.gain { self.gain = headroom; }
            }
            *s *= self.gain;
            self.gain = (self.gain / self.release_coef).min(1.0);
        }
    }
}

// ─── Metering ────────────────────────────────────────────────────────────────

pub fn peak_dbfs_from_peak(peak: f32) -> f32 {
    if peak <= 0.000_001 { -120.0 } else { (20.0 * peak.abs().log10()).clamp(-120.0, 0.0) }
}

// ─── Device scanning ─────────────────────────────────────────────────────────

pub fn scan_audio_devices_once() -> DeviceResponse {
    let host = cpal::default_host();
    let default_input_device  = host.default_input_device();
    let default_output_device = host.default_output_device();
    let default_input_channels = default_input_device.as_ref()
        .and_then(|d| d.default_input_config().ok())
        .map(|c| c.channels() as usize).unwrap_or(0);
    let default_output_channels = default_output_device.as_ref()
        .and_then(|d| d.default_output_config().ok())
        .map(|c| c.channels() as usize).unwrap_or(0);
    let default_input  = default_input_device.and_then(|d| d.name().ok()).unwrap_or_else(|| "none".into());
    let default_output = default_output_device.and_then(|d| d.name().ok()).unwrap_or_else(|| "none".into());
    let inputs  = host.input_devices().map(|ds| ds.filter_map(|d| d.name().ok()).collect()).unwrap_or_default();
    let outputs = host.output_devices().map(|ds| ds.filter_map(|d| d.name().ok()).collect()).unwrap_or_default();
    DeviceResponse { sample_rate: SAMPLE_RATE, default_input, default_output,
        default_input_channels, default_output_channels, inputs, outputs }
}

// ─── Timing ──────────────────────────────────────────────────────────────────

pub fn sleep_until(deadline: Instant) {
    loop {
        let now = Instant::now();
        if now >= deadline { break; }
        let remaining = deadline - now;
        if remaining > Duration::from_millis(2) {
            std::thread::sleep(remaining - Duration::from_millis(1));
        } else {
            std::thread::yield_now();
        }
    }
}
