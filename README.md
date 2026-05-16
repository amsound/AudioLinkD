# AudioLink

Broadcast-grade multichannel audio over IP, built entirely from open standards. RTP framing, Opus codec, plain UDP. The same stack used by BBC, AWS Elemental and Telos Alliance under the hood of their commercial products — assembled here into a clean, auditable Rust implementation with a web-based operator interface.

Up to 64 mono channels per link. 20 ms frames at 48 kHz. Per-channel Opus encode and decode. Timestamp-aligned jitter buffer with Opus PLC for loss concealment. Rubato ASRC for clock drift. Web-based patch matrix for routing and metering. Optional rendezvous server for NAT traversal. Linux first; macOS planned.

-----

## Components

```
.
├── Cargo.toml                       # audiolinkd — the endpoint binary
├── src/main.rs
└─- audiolink-cloud-rendevous/       # audiolink-cloud — rendezvous server
    ├── Cargo.toml
    ├── src/main.rs
    └── audiolink-cloud.service      # systemd unit
```

**`audiolinkd`** runs on each device. It captures audio, encodes per channel, transports over UDP, decodes, and plays back. A web UI on `:8080` handles setup, routing and live stats.

**`audiolink-cloud`** is a small stateless rendezvous server (~300 lines). It tracks `name → public IP:port` registrations with a 30 s TTL, brokers connect requests between peers, and delivers notifications via long-poll. Media never passes through it — it drops out once both sides have each other’s addresses.

-----

## Build

### Dependencies

```bash
sudo apt install build-essential pkg-config libasound2-dev libopus-dev libssl-dev
```

### Endpoint

```bash
cargo build --release
./target/release/audiolinkd
```

Open `http://<device-ip>:8080` to configure.

### Rendezvous server (optional — only needed for NAT traversal)

```bash
cd audiolink-cloud-rendevous
cargo build --release
sudo install -d /opt/audiolink-cloud
sudo install -m 755 target/release/audiolink-cloud /opt/audiolink-cloud/
sudo install -m 644 audiolink-cloud.service /etc/systemd/system/
sudo systemctl enable --now audiolink-cloud
```

The service binds `127.0.0.1:7070`. Point nginx at it with TLS termination. It reads the device’s public IP from the `X-Real-IP` header nginx sets.

### Real-time scheduling

The receive thread requests `SCHED_FIFO` priority 20. Without it the thread can be preempted under load and the buffer underruns. Two ways to enable:

```bash
# Grant the capability directly to the binary
sudo setcap cap_sys_nice+eip ./target/release/audiolinkd

# Or per-user via limits
echo "$USER  -  rtprio  20" | sudo tee -a /etc/security/limits.conf
```

If neither is set, `audiolinkd` logs a warning and runs on `SCHED_OTHER`. It will still work, just less reliably under VM or system load.

-----

## Running

### Web-first (recommended starting point)

```bash
./audiolinkd
```

Starts the web UI on `0.0.0.0:8080`. No network sockets are opened until you configure a remote device in the **Setup** tab and hit **Apply**.

**Apply does a full process rebuild.** The current process `exec()`s the binary with the new arguments from scratch — fresh UDP socket, encoders, decoders, jitter buffer, routing state. There is no in-place reconfiguration. This avoids state leaking between sessions and makes restarts predictable. Settings persist to `audiolinkd_config.json` in the working directory and are reloaded on the next launch.

### Direct IP

One side is the **initiator** (has the remote’s address, fires probes). The other is the **responder** (binds the port, waits). Only one side needs the host configured — either can be either.

Initiator:

```bash
./audiolinkd bidir --remote-host 192.168.1.42 \
                   --remote-name studio-b \
                   --id studio-a \
                   --channels 8 \
                   --bitrate 128000 \
                   --latency-ms 120
```

Responder:

```bash
./audiolinkd bidir --remote-name studio-a \
                   --id studio-b \
                   --channels 8
```

Both sides send probe packets every ~2.13 s. These double as NAT keepalives and liveness checks.

### Cloud / NAT traversal

```bash
./audiolinkd bidir --remote-name studio-b \
                   --rendezvous https://your-rendezvous-server.example.com \
                   --id studio-a \
                   --link-password "shared-secret" \
                   --channels 2
```

Both sides register with the rendezvous server every 10 s. When either calls `/api/connect`, the server returns the remote’s public address and notifies the remote via long-poll. Both endpoints immediately send probes at each other — symmetric NAT hole-punching. The rendezvous server then drops out entirely; all media flows peer-to-peer.

### Flags reference

```
--id <name>               Device name. Defaults to hostname.
--remote-name <name>      Remote device name. Used to derive the shared link token.
--remote-host <ip>        Remote IP. Omit to run as responder.
--channels <n>            Network send channels, 1–64. Default: 2.
--bitrate <bps>           Opus bitrate per channel. Default: 128000.
--latency-ms <ms>         Target receive buffer depth, 5–10000 ms.
                          Floor is 2× the packet size (40 ms for 20 ms frames).
--link-password <pw>      Optional. Combined with device names to derive the shared
                          link token. Neither the password nor the names are transmitted.
--encoder-mode music      Opus Application::Audio, no FEC. Default. Best quality per bit
                          for programme material.
--encoder-mode speech     Opus Application::Voip, inband FEC enabled. Roughly halves
                          perceived loss on flaky paths at ~30–40% bitrate overhead.
                          Transparent to the decoder — no receiver changes needed.
--no-phase-lock           Decode channels independently as packets arrive. Lower latency
                          but no sample-accurate alignment guarantee across channels.
--fixed-jitter            Disable the PI controller. Useful when both ends are externally
                          clocked or for debugging.
--rendezvous <url>        Rendezvous server URL for NAT traversal.
--no-send / --no-recv     One-way operation.
--no-web                  Disable the web UI.
--web <addr:port>         Override web UI bind address. Default: 0.0.0.0:8080.
```

-----

## Architecture

### The stack

AudioLink is a straightforward assembly of well-understood open components:

|Layer           |Component                                            |
|----------------|-----------------------------------------------------|
|Audio I/O       |`cpal` → ALSA (Linux)                                |
|Codec           |`libopus` — per-channel mono, 48 kHz, 20 ms frames   |
|Transport       |Plain UDP, RTP-style framing                         |
|Jitter buffer   |`BTreeMap` keyed by RTP timestamp                    |
|Clock correction|`rubato::FastFixedIn` ASRC, PI-controlled ratio      |
|Web UI          |`axum` HTTP + WebSocket, single-file embedded HTML/JS|
|Rendezvous      |`axum` + `ureq`, long-poll events                    |

### Packet format

12-byte RTP-style header on every packet: `0x80 0x69`, sequence, timestamp (48 kHz clock), SSRC. The SSRC identifies the payload class:

|SSRC        |Class   |Content                                                                         |
|------------|--------|--------------------------------------------------------------------------------|
|`0x00010000`|Media   |`09 06` — one mono Opus frame. Channel index at byte 18.                        |
|`0x00000200`|Control |`09 07` probe / `09 09` accept / `09 08` confirm / `09 0b`+`09 0c` RTT ping-pong|
|`0x00000000`|Metadata|`09 0a` — JSON channel list: `{"id":"…","channels":[{"c":1,"l":"Ch 1"},…]}`     |

One packet per channel per 20 ms frame. Channels are never multiplexed into a single packet — each has its own sequence counter and RTP stream. The receiver classifies any packet in four bytes from the SSRC before parsing further.

### Connection handshake

```
PROBING   send 09 07 every 2.13 s (probe + NAT keepalive)
    │
    ▼  (matching 09 07 received)
ACCEPTING send 09 09 (accept) + 09 08 (confirm) + 09 0a (channel metadata)
    │
    ▼
STREAMING send 09 06 per channel per 20 ms
    │
    ▼
KEEPALIVE 09 07 continues every 2.13 s
```

The responder doesn’t `connect()` its UDP socket to the remote until **after** a token-validated handshake. Connecting earlier would install a kernel-level peer filter that could latch onto the wrong NAT-mapped port before the real one is known.

The `09 0a` metadata packet carries the sender’s channel count and per-channel labels. The receiver sizes its decoder bank from this — sender and receiver channel counts are fully independent. Change `--channels` on one side and the other reconfigures on the next metadata packet.

### Three-state peer status

- **Gray** — no peer. Either nothing configured or the remote hasn’t responded.
- **Green** — connected via the configured direct path.
- **Orange** — connected but via a different address (rendezvous / NAT-punched path active) or recent loss/underrun. Informational, not an error.

### Why one mono Opus stream per channel

Each network channel is a separate UDP stream with its own encoder, decoder, sequence counter and ring buffer.

- A dropped packet on ch 3 doesn’t affect ch 0–2.
- Per-channel PLC: Opus concealment runs independently per channel on loss.
- Sample-accurate phase alignment falls naturally from the shared RTP timestamp — all packets for the same 20 ms frame carry the same timestamp, so the receiver can group them before decoding.
- The receiver accepts any channel index 0–63 off the wire regardless of how many channels the local node is sending.

The per-packet header overhead doesn’t matter at these bitrates.

### Phase-locked jitter buffer

Packets accumulate in a `BTreeMap<rtp_timestamp, FrameGroup>`. A group is ready when either all expected channels for that timestamp have arrived, or 10 ms has elapsed — one frame duration. The group then decodes in one pass; any missing channels get Opus PLC.

A high-watermark of the last drained timestamp prevents late-arriving real packets from re-decoding a timestamp that already got PLC. Without this, the late packet creates a fresh map entry, decodes again, and pushes fill above real-time rate.

Phase lock is essential for stereo pairs and surround mixes. For independent mono channels (talkback, IFB, separate feeds) `--no-phase-lock` decodes per channel as it arrives for lower latency.

### Receive buffer floor

Effective buffer depth is clamped to 2× the packet size — 40 ms for 20 ms frames — regardless of the `--latency-ms` setting. Requests below that are silently raised. This prevents underruns when operators set unrealistically low targets on clean networks.

### Clock drift correction

Sender and receiver clocks are never identical — typically ±50 ppm, which accumulates to ~28 ms over 10 minutes at 48 kHz if uncorrected. The receive thread runs a PI controller on playback ring fill depth:

- **P term** responds immediately to fill error.
- **I term** integrates at 5 Hz, anti-windup clamped at ±12000 ppm. Eliminates the steady-state fill error a P-only loop leaves behind when there’s a constant ppm offset between clocks.

The output is a resample ratio fed into `rubato::FastFixedIn` per channel before samples hit the playback ring. `PolynomialDegree::Linear` is audibly transparent at sub-500 ppm corrections. A 3× target overflow backstop resets the integral and logs a warning, throttled to once per 10 s.

### Buffer recommendation

The receiver tracks p95 inter-packet arrival jitter over a rolling ~60 s window (300 samples at 50 fps) and reports `recommended_buffer_ms = clamp(p95 × 4, 80, 500)`. The EMA displayed alongside it smooths bursts too aggressively for buffer-sizing decisions — p95 is more conservative and more useful. The 80 ms floor accounts for WiFi behaviour (power-save wakeups, channel scans) which doesn’t reliably appear in a short p95 window but will cause glitches on a too-tight buffer.

### Patch matrix and routing

Sources × destinations grid. Click crosspoints to connect. Many-to-one sums; one-to-many splits.

```
Source types                 Destination types
──────────────────           ──────────────────────
input:N    (capture ch)      output:N  (playback ch)
peer:remote:ch:N (decoded)   stream:0:ch:N (network send)
ebu:l / ebu:r (test tone)
```

Routes are stored as `(source, destination)` string pairs and applied as 64-bit atomic bitmasks the audio threads read with `Ordering::Relaxed` — no locks in the hot path. Nothing transmits or monitors until a crosspoint is made. The EBU R49 1 kHz line-up tone generator runs continuously for metering but only reaches the network or speakers when explicitly patched.

Many-to-one summing divides by contributor count:

```
output = sum(contributing channels) / contributor_count
```

This keeps unity gain on a single active source while preventing clipping when multiple channels feed the same destination. No hidden gain stages anywhere — audio is numerically untouched except Opus encode/decode and this sum/normalise.

Network receive channels appear automatically as source rows when a peer sends its channel metadata, labelled with whatever names the remote configured. Crosspoint state is remembered across disconnects and restored on reconnect.

### NAT traversal detail

The endpoint socket is left **unconnected** during rendezvous punching, using `send_to` instead of `connect`+`send`. `connect()` to the rendezvous-registered port would install a kernel peer filter that silently drops packets arriving from the remote’s actual NAT-mapped port, which on symmetric NATs is almost always a different port. The socket is only `connect()`ed once a token-validated handshake has arrived and the real source address is confirmed.

### Staleness watchdog

If connected but no valid packet arrives for 15 s:

- **Responder mode:** `connect(AF_UNSPEC)` removes the kernel peer filter so `recv_from` accepts from any source again — the recovery path for NAT mapping changes (mobile handoff, network switch, firewall test).
- **Initiator mode:** state resets; the socket stays connected; probes restart.

An epoch counter is bumped either way. The receive thread observes it and flushes decoders, drains stale ring contents under silence, and re-primes from scratch so old session audio doesn’t mix into the new one.

### Audio callback discipline

No heap allocation and no mutex locking inside any audio callback. Callbacks are memcpy-only.

Opus encoding runs on a dedicated thread. Encode time varies with signal complexity — running it inside the hardware-clock callback causes modulation-correlated clicks. Pre-allocated buffers move via lock-free SPSC rings (`ringbuf::HeapRb`).

`BufferSize::Fixed(960)` — one 20 ms frame. `BufferSize::Default` gives ALSA periods of 1–2 seconds. PipeWire/PulseAudio on VMs may non-deterministically double or quadruple the requested period size; bare metal is well-behaved.

-----

## Configuration

`audiolinkd_config.json` in the working directory. Written by the web UI’s Apply action; read at startup so the node comes up in the same state.

```json
{
  "config": {
    "remote": "192.168.1.42",
    "remote_device_name": "studio-b",
    "link_password": "...",
    "node_id": "studio-a",
    "channels": 8,
    "opus_bitrate_per_channel": 128000,
    "latency_ms": 120,
    "phase_lock": true,
    "encoder_mode": "music",
    "channel_labels": ["Mic 1", "Mic 2"],
    "rendezvous_url": "https://audiolink.example.com"
  },
  "routes": [
    { "source": "input:0",          "destination": "stream:0:ch:0" },
    { "source": "peer:remote:ch:0", "destination": "output:0" }
  ]
}
```

Channel labels are persisted separately from the rest of the config so an Apply rebuild doesn’t wipe them.

-----

## Environment variables

`AUDIOLINK_DSCP_EF=1` — set DSCP EF (`IP_TOS=0xb8`) on the UDP socket. Off by default. On unmanaged internet paths DSCP is often ignored or remarked by transit, and some networks police it in ways that make loss worse. Only enable on a QoS-aware managed path.

`RUST_LOG=info` (or `debug`, `trace`) — log level via `tracing-subscriber`. `info` is suitable for normal operation; `debug` adds per-handshake and per-RTT detail.

-----

## Roadmap

|M |Milestone                                    |Status                                                                                                |
|--|---------------------------------------------|------------------------------------------------------------------------------------------------------|
|1 |Audio loopback                               |✓                                                                                                     |
|2 |One-way UDP                                  |✓                                                                                                     |
|3 |Bidirectional audio                          |✓                                                                                                     |
|4 |Multichannel (up to 64)                      |✓                                                                                                     |
|5 |Handshake state machine + channel metadata   |✓                                                                                                     |
|6 |Timestamped jitter buffer + ASRC + phase lock|✓                                                                                                     |
|7 |Web UI + patch matrix                        |✓                                                                                                     |
|8 |mDNS LAN discovery                           |Dropped — direct IP and rendezvous cover the deployment paths                                         |
|9 |Rendezvous server                            |✓                                                                                                     |
|10|Resilience options (FEC / duplicate send)    |Partial — Opus inband FEC via `--encoder-mode speech`; duplicate-send and per-channel selector not yet|

### Not yet implemented

- **macOS / CoreAudio** — planned after the Linux feature set stabilises
- **Transport encryption** — AES-256-GCM; key material derivation is already in place
- **Multi-peer per endpoint** — architecture supports it; not wired up
- **Per-channel resilience mode** — encoder mode is currently global per endpoint
- **Web UI authentication** — bind to localhost and tunnel until this lands
- **Mix bus virtual endpoints** — many-to-one into physical/network destinations works today via the matrix; named virtual buses are not yet a separate endpoint type

-----

## License

TBD.
