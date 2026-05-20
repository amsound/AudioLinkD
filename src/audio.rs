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

/// Temporarily redirect stderr to /dev/null on Linux to suppress the wall of
/// ALSA "cannot connect to JACK / PulseAudio / OSS" messages that libasound
/// writes directly when cpal probes virtual backends during enumeration.
/// The file descriptors are restored immediately after the call.
#[cfg(target_os = "linux")]
pub fn suppress_alsa_stderr<F: FnOnce() -> T, T>(f: F) -> T {
    use std::os::unix::io::AsRawFd;
    let stderr_fd = std::io::stderr().as_raw_fd();
    let saved = unsafe { libc::dup(stderr_fd) };
    if saved < 0 { return f(); }
    if let Ok(devnull) = std::fs::OpenOptions::new().write(true).open("/dev/null") {
        unsafe { libc::dup2(devnull.as_raw_fd(), stderr_fd); }
    }
    let result = f();
    unsafe { libc::dup2(saved, stderr_fd); libc::close(saved); }
    result
}

#[cfg(not(target_os = "linux"))]
pub fn suppress_alsa_stderr<F: FnOnce() -> T, T>(f: F) -> T { f() }

pub fn scan_audio_devices_once() -> DeviceResponse {
    suppress_alsa_stderr(|| scan_audio_devices_inner())
}

fn scan_audio_devices_inner() -> DeviceResponse {
    let host = cpal::default_host();
    // Force 48kHz on default devices before querying config —
    // ensures the scan reflects the actual operating rate.
    if let Some(d) = host.default_output_device() {
        if let Ok(n) = d.name() { try_force_device_sample_rate(&n, SAMPLE_RATE); }
    }
    if let Some(d) = host.default_input_device() {
        if let Ok(n) = d.name() { try_force_device_sample_rate(&n, SAMPLE_RATE); }
    }
    let default_input_device  = host.default_input_device();
    let default_output_device = host.default_output_device();
    let default_input_channels = default_input_device.as_ref()
        .and_then(|d| d.default_input_config().ok())
        .map(|c| c.channels() as usize).unwrap_or(0);
    let default_input_sample_rate = default_input_device.as_ref()
        .and_then(|d| d.default_input_config().ok())
        .map(|c| c.sample_rate().0).unwrap_or(SAMPLE_RATE);
    let default_output_channels = default_output_device.as_ref()
        .and_then(|d| d.default_output_config().ok())
        .map(|c| c.channels() as usize).unwrap_or(0);
    let default_output_sample_rate = default_output_device.as_ref()
        .and_then(|d| d.default_output_config().ok())
        .map(|c| c.sample_rate().0).unwrap_or(SAMPLE_RATE);
    let default_input  = default_input_device.and_then(|d| d.name().ok()).unwrap_or_else(|| "none".into());
    let default_output = default_output_device.and_then(|d| d.name().ok()).unwrap_or_else(|| "none".into());
    let inputs  = host.input_devices().map(|ds| ds.filter_map(|d| d.name().ok()).collect()).unwrap_or_default();
    let outputs = host.output_devices().map(|ds| ds.filter_map(|d| d.name().ok()).collect()).unwrap_or_default();
    DeviceResponse { sample_rate: SAMPLE_RATE, default_input, default_output,
        default_input_channels, default_output_channels,
        default_input_sample_rate, default_output_sample_rate,
        inputs, outputs }
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

// ─── Device config helpers ────────────────────────────────────────────────────

/// On macOS, attempt to set the CoreAudio device's nominal sample rate to
/// `rate` Hz before cpal opens a stream.  This overrides whatever Audio MIDI
/// Setup has selected.  Other platforms: no-op.
///
/// Requires the `coreaudio-sys` crate in Cargo.toml:
///   [target.'cfg(target_os = "macos")'.dependencies]
///   coreaudio-sys = "0.2"
pub fn try_force_device_sample_rate(_device_name: &str, _rate: u32) {
    #[cfg(target_os = "macos")]
    {
        use std::mem;
        // We need the CoreAudio device ID. Enumerate all devices and match by name.
        let result = (|| -> Result<(), Box<dyn std::error::Error>> {
            use coreaudio_sys::*;

            // Get all audio device IDs
            let property_address = AudioObjectPropertyAddress {
                mSelector: kAudioHardwarePropertyDevices,
                mScope: kAudioObjectPropertyScopeGlobal,
                mElement: kAudioObjectPropertyElementMaster,
            };
            let mut data_size: u32 = 0;
            let status = unsafe {
                AudioObjectGetPropertyDataSize(
                    kAudioObjectSystemObject,
                    &property_address, 0, std::ptr::null(), &mut data_size)
            };
            if status != 0 { return Err(format!("GetPropertyDataSize: {status}").into()); }

            let device_count = data_size as usize / mem::size_of::<AudioDeviceID>();
            let mut device_ids = vec![0u32; device_count];
            let status = unsafe {
                AudioObjectGetPropertyData(
                    kAudioObjectSystemObject, &property_address,
                    0, std::ptr::null(), &mut data_size,
                    device_ids.as_mut_ptr() as *mut _)
            };
            if status != 0 { return Err(format!("GetPropertyData devices: {status}").into()); }

            // Find device matching our name
            for &device_id in &device_ids {
                let name_addr = AudioObjectPropertyAddress {
                    mSelector: kAudioObjectPropertyName,
                    mScope: kAudioObjectPropertyScopeGlobal,
                    mElement: kAudioObjectPropertyElementMaster,
                };
                let mut cf_name: coreaudio_sys::CFStringRef = std::ptr::null();
                let mut sz = mem::size_of::<coreaudio_sys::CFStringRef>() as u32;
                let status = unsafe {
                    AudioObjectGetPropertyData(device_id, &name_addr, 0, std::ptr::null(),
                        &mut sz, &mut cf_name as *mut _ as *mut _)
                };
                if status != 0 || cf_name.is_null() { continue; }

                // Convert CFString to Rust String
                let name_len = unsafe { coreaudio_sys::CFStringGetLength(cf_name) } as usize;
                let mut buf = vec![0u8; name_len * 4 + 4];
                let ok = unsafe {
                    coreaudio_sys::CFStringGetCString(
                        cf_name, buf.as_mut_ptr() as *mut _, buf.len() as _,
                        coreaudio_sys::kCFStringEncodingUTF8)
                };
                if ok == 0 { continue; }
                let name = std::ffi::CStr::from_bytes_until_nul(&buf)
                    .map(|s| s.to_string_lossy().into_owned())
                    .unwrap_or_default();

                if name != _device_name { continue; }

                // Found it — set the nominal sample rate
                let rate_addr = AudioObjectPropertyAddress {
                    mSelector: kAudioDevicePropertyNominalSampleRate,
                    mScope: kAudioObjectPropertyScopeGlobal,
                    mElement: kAudioObjectPropertyElementMaster,
                };
                let rate_f64 = _rate as f64;
                let sz = mem::size_of::<f64>() as u32;
                let status = unsafe {
                    AudioObjectSetPropertyData(device_id, &rate_addr, 0, std::ptr::null(),
                        sz, &rate_f64 as *const f64 as *const _)
                };
                if status == 0 {
                    tracing::info!("CoreAudio: set '{name}' nominal rate to {_rate}Hz");
                } else {
                    tracing::warn!("CoreAudio: set rate failed for '{name}': status {status}");
                }
                return Ok(());
            }
            tracing::warn!("CoreAudio: device '{_device_name}' not found for rate-forcing");
            Ok(())
        })();
        if let Err(e) = result {
            tracing::warn!("CoreAudio rate-force error: {e}");
        }
    }
}

/// Returns `(stream_config, device_sample_rate)`.
///
/// Strategy: always attempt to open at 48kHz first by checking the supported
/// config ranges. If the driver reports 48kHz is in range, we use it — this
/// works even when the default config is at another rate (e.g. PipeWire
/// configured at 44100 but hardware supports 48kHz).
///
/// If 48kHz is genuinely unsupported (e.g. Bluetooth HFP mic at 16/24kHz),
/// we fall back to the device's native rate and resample in software.
pub fn best_input_config(device: &cpal::Device, preferred_channels: usize) -> (cpal::StreamConfig, u32) {
    use cpal::traits::DeviceTrait;
    let native = device.default_input_config()
        .map(|c| (c.sample_rate().0, c.channels()))
        .unwrap_or((SAMPLE_RATE, preferred_channels as u16));
    let channels = (preferred_channels as u16).min(native.1).max(1);

    // Check supported ranges — ask for 48kHz in the supported range rather than
    // just accepting the default.  PipeWire often reports 44100 as default but
    // will happily run at 48kHz if asked.
    let supported_48 = device.supported_input_configs().ok()
        .map(|cfgs| cfgs.into_iter().any(|c|
            c.channels() >= channels
            && c.min_sample_rate().0 <= SAMPLE_RATE
            && c.max_sample_rate().0 >= SAMPLE_RATE))
        .unwrap_or(false);

    let rate = if supported_48 {
        if native.0 != SAMPLE_RATE {
            tracing::info!(
                "Input: device default is {}Hz but 48kHz is supported — requesting 48kHz",
                native.0
            );
        }
        SAMPLE_RATE
    } else {
        tracing::warn!(
            "Input: {}Hz not supported — using {}Hz + software resample to 48kHz",
            SAMPLE_RATE, native.0
        );
        native.0
    };
    (cpal::StreamConfig { channels, sample_rate: cpal::SampleRate(rate), buffer_size: ALSA_PERIOD }, rate)
}

pub fn best_output_config(device: &cpal::Device, preferred_channels: u16) -> (cpal::StreamConfig, u32) {
    use cpal::traits::DeviceTrait;
    let native = device.default_output_config()
        .map(|c| (c.sample_rate().0, c.channels()))
        .unwrap_or((SAMPLE_RATE, preferred_channels));

    let supported_48 = device.supported_output_configs().ok()
        .map(|cfgs| cfgs.into_iter().any(|c|
            c.channels() >= preferred_channels
            && c.min_sample_rate().0 <= SAMPLE_RATE
            && c.max_sample_rate().0 >= SAMPLE_RATE))
        .unwrap_or(false);

    let rate = if supported_48 {
        if native.0 != SAMPLE_RATE {
            tracing::info!(
                "Output: device default is {}Hz but 48kHz is supported — requesting 48kHz",
                native.0
            );
        }
        SAMPLE_RATE
    } else {
        tracing::warn!(
            "Output: {}Hz not supported — using {}Hz + software resample from 48kHz",
            SAMPLE_RATE, native.0
        );
        native.0
    };
    (cpal::StreamConfig { channels: preferred_channels, sample_rate: cpal::SampleRate(rate), buffer_size: ALSA_PERIOD }, rate)
}

/// Build a Rubato resampler from `from_rate` to `to_rate` with `channels` channels.
/// Returns `None` if rates are equal (no resampling needed).
pub fn make_io_resampler_n(from_rate: u32, to_rate: u32, channels: usize) -> Option<rubato::FastFixedIn<f32>> {
    if from_rate == to_rate { return None; }
    let ratio = to_rate as f64 / from_rate as f64;
    let chunk = (from_rate as usize * 20) / 1000;
    Some(rubato::FastFixedIn::new(ratio, 1.1, rubato::PolynomialDegree::Linear, chunk, channels)
        .expect("IO resampler init failed"))
}

/// Mono resampler — convenience wrapper for input capture path.
pub fn make_io_resampler(from_rate: u32, to_rate: u32) -> Option<rubato::FastFixedIn<f32>> {
    make_io_resampler_n(from_rate, to_rate, 1)
}
