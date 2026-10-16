# Bairelay

## What this is

Bairelay bridges Reolink battery cameras (Argus-class) to standard protocols. It exposes RTSP for video and uses MQTT for camera state, control, and Home Assistant discovery. Battery cameras wake on demand and sleep again after the RTSP session ends, so HA cards and automations work without keeping the camera awake all the time.

## Installation

- Go to **Settings → Apps** and click **Install app** (bottom-right).
- Click the three-dot menu (⋮) → **Repositories**.
- Paste `https://github.com/mgc8/bairelay` and click **Add**.
- The Bairelay app appears in the list. Click it, then **Install**.
- Switch to the **Configuration** tab and fill in `topic_prefix`, `log_level`, and one entry per camera.
- **Start**. The app picks up HA's Mosquitto integration automatically (broker, port, credentials) and auto-publishes Home Assistant MQTT discovery payloads under the `homeassistant/` prefix — entities show up in HA without further configuration.

## Configuration: options form

| Field | Type | Description |
|-------|------|-------------|
| `topic_prefix` | string | MQTT topic root, default `bairelay`. Set to `neolink` for drop-in compatibility with an existing neolink deployment. |
| `log_level` | enum: `info` / `debug` / `trace` | Default `info`. `debug` surfaces protocol-level traces; `trace` is everything. |
| `cameras` | list | One entry per camera. Each entry has the sub-fields below. |

Each camera entry:

| Sub-field | Required | Description |
|-----------|----------|-------------|
| `name` | yes | Alphanumeric, underscore, or hyphen. Used as the camera's MQTT topic segment and HA entity name. |
| `address` | one of address/uid | LAN IP or hostname (e.g. `192.168.1.50`). Tried first; UID discovery is the fallback when `uid` is also set. |
| `uid` | one of address/uid | Reolink P2P UID (16 alphanumeric characters, e.g. `9527000ABCDEF123`). Used for discovery when `address` is blank or fails. |
| `username` | no | Camera account username. Defaults to `admin` (Reolink's stock account). |
| `password` | yes | The camera account password. |
| `idle_disconnect` | no | Drop the camera connection when no clients are streaming, letting battery cameras sleep. Defaults to `true`. Turn off for always-on cameras you want kept warm. |

Fill in `address`, `uid`, or both. With both, bairelay connects to `address` first and falls back to UID discovery when that fails, including when a hostname fails to resolve.

## Configuration: TOML file

The full, editable configuration lives at `/config/bairelay/config.toml`. On the app's first start, bairelay writes an annotated template there with every available option documented inline. After that, the file is yours — bairelay never rewrites it.

- Use the **File editor** app, the **Terminal** app, or SSH to edit it.
- On every start, bairelay merges the options form (base) with this file (overlay). The file overrides the form field-by-field; cameras are matched by `name`.
- MQTT broker credentials from HA's Mosquitto integration are auto-injected at startup. To use an external broker instead, add a full `[mqtt]` block to the file. For TLS to it, set `tls = true` (system CAs) or `ca` (a private CA), plus `client_auth` for mutual TLS; keep certificate files under `/ssl` so the app can read them.
- HA MQTT discovery is enabled by default with topic prefix `homeassistant`. To opt out or change the prefix, add `[mqtt.discovery] topic = "your-prefix"` or remove the `[mqtt]` block entirely (RTSP-only mode).
- The exact merged configuration bairelay loaded is written to `/config/bairelay/effective.toml` on every start (read-only diagnostic; defaults stripped so you see only what's actually set). Don't edit that file — your changes will be overwritten next restart.
- For the canonical reference, see `sample_config.toml` in the bairelay repo: <https://github.com/mgc8/bairelay/blob/main/sample_config.toml>.

## Worked example

To enable the wake server and turn on the floodlight on a camera called `Hallway` (already configured in the form), add this to `/config/bairelay/config.toml`:

```toml
[wake_server]
enable = true

[[cameras]]
name = "Hallway"            # must match the form's `name` exactly
enable_floodlight = true
```

The `name` field is the only required key for a `[[cameras]]` override — the merge layer keeps every other base value (username, password, etc.) from the form unless you explicitly restate them here.

## Troubleshooting

### Cameras don't appear in Home Assistant

Check that the MQTT integration is installed and enabled in HA: **Settings → Devices & Services**. The add-on auto-discovers it via the `mqtt:want` service link. Also check the add-on's **Log** tab — connection failures surface there.

### Battery cameras never wake or never connect

Bairelay reaches cameras one of two ways: by LAN address (`address = "<ip>"` with `discovery = "remote"`), or by Reolink P2P UID (`uid = "..."` with `discovery = "relay"`). Either path can fail depending on your network — the LAN-direct route needs the camera awake and reachable on the same subnet as Home Assistant, while the P2P route needs both your host and the camera to reach Reolink's cloud servers. If one combination doesn't connect, swap to the other and restart the app.

### Logs

The **Log** tab on the add-on page shows the running output. Set `log_level: debug` in the options to surface bairelay-internal protocol traces, or `trace` for noisier per-packet detail. The add-on entrypoint sets `RUST_LOG` automatically based on `log_level`.

## Network ports

The add-on uses host networking. By default it binds:

- `8554/tcp` — plain RTSP (always on).
- `8555/tcp` — `rtsps://` (only when the overlay sets `certificate = "..."`).
- `9999/udp` and `58200/udp` — wake server (only when `[wake_server] enable = true`).

The push listener (a separate motion-detection mechanism that intercepts the camera's `pushx.reolink.com` connection) is opt-in and configurable to any port. Configure it in the overlay only if you need it.

The webhook listener (`8780/tcp` by default) is opt-in through `[webhook]` in the overlay plus `webhook = true` on a battery doorbell. It lets a sleeping doorbell report button presses. The doorbell holds one webhook URL, which Home Assistant's Reolink integration also uses, so enable it only for a doorbell that integration doesn't manage. See the bairelay README's "Battery doorbell webhook" section.

## Repository

Source code, issue tracker, full sample config, and deeper docs: <https://github.com/mgc8/bairelay>.
