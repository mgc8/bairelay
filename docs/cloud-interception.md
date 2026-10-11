# Cloud interception

How bairelay replaces or intercepts the cloud-side services that battery Reolink cameras depend on, so a fully-local deployment can wake the camera *and* learn about motion events without involving Reolink's infrastructure.

There are two independent interception paths, both reverse-engineered from packet captures of real Argus hardware against Reolink's own cloud — Reolink has published nothing about either. The two paths share the same operator-side mechanism (LAN DNS redirect of specific Reolink hostnames to the bairelay box) and the same in-process `CameraRegistry` mapping camera UID ↔ source IP, but otherwise serve distinct roles:

- **Part I — wake server.** A pair of UDP listeners (ports 9999, 58200) that replace `p2p*.reolink.com`. Maintains the camera's session-state, keeps the NAT pinhole open via heartbeats, fires the wake packet when an operator wants the camera on demand. Without this, sleeping battery cameras cannot be woken locally — the BcUdp discovery protocol is the only firmware-supported wake mechanism.
- **Part II — motion-push listener.** A TCP listener (port 443) that replaces `pushx.reolink.com`. Treats the camera's motion-time HTTPS connect attempt as the motion signal — we don't decode the body (the camera pins the cert chain) but the connection attempt itself is enough to fire `status/motion=on` locally and acquire a wake-lock so the connect loop reconnects via the Part I wake server. Without this, motion that happens while bairelay is disconnected goes unnoticed locally; the only path the camera has to push the event is to Reolink's cloud.

Target hardware: Argus-class battery cameras on firmware `v3.0.x`.

---

## Table of contents

- [Part I — wake server](#part-i--wake-server)
  - [I.1 Why the protocol exists](#i1-why-the-protocol-exists)
  - [I.2 Wire format](#i2-wire-format)
  - [I.3 Server topology](#i3-server-topology)
  - [I.4 Camera-side handshake (boot sequence)](#i4-camera-side-handshake-boot-sequence)
  - [I.5 Wake-on-demand sequence](#i5-wake-on-demand-sequence)
  - [I.6 Field-level subtleties](#i6-field-level-subtleties)
  - [I.7 Operator deployment](#i7-operator-deployment)
  - [I.8 Implementation pointers](#i8-implementation-pointers)
  - [I.9 Open questions](#i9-open-questions)
- [Part II — motion-push listener](#part-ii--motion-push-listener)
  - [II.1 What the camera does on motion](#ii1-what-the-camera-does-on-motion)
  - [II.2 Why we don't terminate TLS](#ii2-why-we-dont-terminate-tls)
  - [II.3 Connect attempt as the signal](#ii3-connect-attempt-as-the-signal)
  - [II.4 End-to-end flow](#ii4-end-to-end-flow)
  - [II.5 Operator deployment](#ii5-operator-deployment)
  - [II.6 Implementation pointers](#ii6-implementation-pointers)
  - [II.7 Open questions](#ii7-open-questions)

---

# Part I — wake server

XML schemas live in `crates/core/src/bcudp/xml.rs`; listener loops in `crates/wake-server/`. Round-trip tests in `crates/core/src/bcudp/model_tests.rs` pin the wire-format invariants. The `m2dqr_serialises_byte_for_byte_with_real_cloud` test pins the exact `M2D_Q_R` element shape Reolink's cloud emits.

## I.1 Why the protocol exists

Battery-powered Reolink cameras do not stay listening on TCP 9000 while sleeping. Power is too tight. To preserve the ability to wake them on demand, the camera maintains a lightweight UDP heartbeat to a registration server "in the cloud". That server keeps the camera's NAT pinhole open and, when a client wants to connect, fires a wake packet through the existing pinhole. The camera receives it, comes online, and the client then performs the regular Baichuan handshake on TCP 9000.

Reolink's cloud servers (middleman on AWS EC2 `eu-west-3`, register and log on Linode) are the only mechanism by which a sleeping battery camera receives a wake packet. Local broadcast discovery (UDP 2018) only finds cameras that are already awake. There is no other cloud-bypass mechanism in firmware.

Bairelay's wake server replaces those cloud servers locally. With operator-side DNS redirecting `p2p*.reolink.com` to the bairelay box, both cameras and bairelay's own outgoing discovery hit the local listener — no code change in either, just DNS.

## I.2 Wire format

### I.2.1 BcUdp Discovery framing

Every packet on UDP ports 9999 and 58200 follows the same envelope. A 20-byte little-endian header followed by an XOR-encrypted XML payload:

```
offset  size  field
  0       4   magic = 0x2a87cf3a
  4       4   payload_size (encrypted body length, bytes)
  8       4   unknown, always 1
 12       4   tid (transmission id; doubles as XOR key offset)
 16       4   crc32 of encrypted body (CRC-32, polynomial 0x04c11db7,
              init = 0, xorout = 0)
 20       N   encrypted XML body (length = payload_size)
```

Implemented in `crates/core/src/bcudp/{ser,de}.rs`. The CRC is the same non-standard variant the rest of the Baichuan stack uses (`crates/core/src/bcudp/crc.rs`): `crc32fast::Hasher::new_with_initial(0xffffffff).finalize() ^ 0xffffffff`.

### I.2.2 XOR keystream

The XML body is XOR-encrypted with a 32-byte keystream derived from 8 hard-coded `u32` constants plus the packet's `tid`:

```
KEY = [0x1f2d3c4b, 0x5a6c7f8d, 0x38172e4b, 0x8271635a,
       0x863f1a2b, 0xa5c6f7d8, 0x8371e1b4, 0x17f2d3a5]
keystream = concat( (k.wrapping_add(tid)).to_le_bytes() for k in KEY )  // 32 bytes
plaintext[i] = ciphertext[i] XOR keystream[i % 32]
```

Wrapping arithmetic is required: `tid` values `>= 0x60000000` overflow `u32` if ordinary `+` is used, and any random `u32` `tid` is in range. Encryption is symmetric — encrypt and decrypt use the same keystream.

### I.2.3 XML envelope

The decrypted payload is always a single-element XML document wrapped in `<P2P>`:

```xml
<P2P>
  <SOMETHING>
    ...
  </SOMETHING>
</P2P>
```

quick-xml deserialises tolerantly: unknown sub-elements are silently dropped. `D2R_DISC` payloads in particular pack `<time>`, `<send>`, `<recv>` diagnostic blobs that do not need to round-trip.

## I.3 Server topology

Two UDP listeners. Both speak BcUdp Discovery framing.

### I.3.1 Middleman — UDP 9999

Where every peer goes first. Two query types arrive:

- `C2M_Q` from clients (neolink, bairelay's own relay/map discovery, the official mobile app). Reply: `M2C_Q_R`.
- `D2M_Q` from cameras on first boot, before the camera has a register address to talk to. Reply: `M2D_Q_R`.

Both replies tell the peer where the register server is. **The schemas differ.** `M2C_Q_R` carries four `IpPort` fields (`reg`, `relay`, `log`, `t`) and no anchors; `M2D_Q_R` carries two `IpPort` fields (`reg`, `log`) plus `<timer/>`, `<retry/>`, `rsp`, `token`, `ac`. Cameras silently reject an `M2D_Q_R` shaped like `M2C_Q_R`.

### I.3.2 Register — UDP 58200

Steady-state camera traffic and the wake handshake live here.

- `D2R_R` from cameras to anchor a session — sent immediately after `M2D_Q_R`. Reply: `R2D_R_R`.
- `D2R_HB` from cameras every ~10 s thereafter (firmware-controlled cadence; the `<hb>20000</hb>` value in `R2D_HB_R` is advisory and cameras pick their own cadence). Reply: `R2D_HB_R`.
- `C2R_C` from clients asking to wake a UID. The server looks the UID up in the registry, fires 10 × `R2D_C` at the camera at 100 ms intervals, and replies `R2C_C_R` + `R2C_T` to the client.
- `D2R_C_R` from cameras acknowledging the wake (informational; log only).
- `D2R_DISC` from cameras with end-of-session diagnostics. Reply: `R2D_DC_R`.
- `C2R_CFM` from clients confirming session setup (informational; log only).

### I.3.3 Cloud topology, for reference

Reolink's actual deployment as observed on the wire:

| Service          | Address                                  | Port  |
|------------------|------------------------------------------|-------|
| Middleman        | AWS EC2 `eu-west-3` (e.g. `15.188.197.53`) | 9999  |
| Register         | Linode (e.g. `172.239.26.140`)           | 58200 |
| Log / telemetry  | same Linode host                         | 57850 |

Bairelay's wake server collapses all three into a single listener pair. Cameras do not appear to mind that the `<log>` field in `M2D_Q_R` points at the register address; no log connection has been observed in any capture.

## I.4 Camera-side handshake (boot sequence)

The full sequence a battery camera goes through after power-on, before it can be woken on demand. Numbers in parentheses are observed UDP-payload byte counts.

```
0. Camera boots and joins its wifi network.

1. Camera resolves p2p.reolink.com via DNS.
   With operator-side LAN DNS redirect, this returns the bairelay box IP.

2. Camera -> middleman:9999  (D2M_Q, ~85 B)
       <P2P><D2M_Q><uid>9527000XXXXXXXXX0123</uid><r>2</r></D2M_Q></P2P>
   uid is the long form (16-char base + 4-char firmware suffix).
   r is a firmware revision (observed values: 2, 3).

3. middleman -> camera        (M2D_Q_R, ~226 B)
       <P2P><M2D_Q_R>
         <reg><ip>...</ip><port>58200</port></reg>
         <log><ip>...</ip><port>57850</port></log>
         <timer/>
         <retry/>
         <rsp>0</rsp>
         <token>...random u64...</token>
         <ac>...random u32...</ac>
       </M2D_Q_R></P2P>
   Field order matters. Empty <timer/> and <retry/> are required markers.
   The token and ac anchor the session; the camera echoes both back.

4. Camera -> register:58200   (D2R_R, ~110 B)
       <P2P><D2R_R>
         <uid>9527000XXXXXXXXX0123</uid>
         <token>...echoed from M2D_Q_R...</token>
         <r>2</r>
       </D2R_R></P2P>

5. register -> camera         (R2D_R_R, ~82 B)
       <P2P><R2D_R_R>
         <rsp>-4</rsp>
         <ac>...echoed from M2D_Q_R...</ac>
       </R2D_R_R></P2P>
   rsp = -4 is informational, NOT fatal. Cameras accept this and proceed.
   The ac MUST match what was sent in step 3 or the camera silently
   re-queries D2M_Q. Per-UID anchor tracking lives in
   crates/wake-server/src/registry.rs::SessionAnchors.

6. Camera -> register:58200   (D2R_HB, ~125 B; every ~10 s)
       <P2P><D2R_HB>
         <uid>9527000XXXXXXXXX0123</uid>
         [<dev><ip>...</ip><port>...</port></dev>]
         <needrsp>1</needrsp>
         <token>...session token...</token>
       </D2R_HB></P2P>
   <dev> is the camera self-reported LAN address; do NOT use it as the
   reply target — see §6.1. Some firmwares omit <dev>.

7. register -> camera         (R2D_HB_R, ~82-180 B)
       <P2P><R2D_HB_R>
         <rsp>0</rsp>
         <time_t>...unix epoch...</time_t>
         <timer><hb>20000</hb></timer>
       </R2D_HB_R></P2P>
   Sent only when the heartbeat had needrsp=1. The <hb> value is
   advisory; cameras pick their own cadence (~10 s observed).
```

Steps 6–7 repeat indefinitely while the camera sleeps. Steps 1–5 happen on power-on. Mid-session re-anchoring has not been observed.

## I.5 Wake-on-demand sequence

What happens when something — bairelay's MQTT wakeup handler, neolink, the official Reolink app — wants to wake a camera that is currently sleeping. The camera is already in steady-state heartbeats (§4 step 6).

```
1. Client resolves p2p.reolink.com via DNS.

2. Client -> middleman:9999   (C2M_Q, ~85 B)
       <P2P><C2M_Q><uid>9527000XXXXXXXXX</uid><p>MAC</p></C2M_Q></P2P>
   Note: the SHORT form UID (16 chars). Bairelay constructs this from
   the operator's config; neolink does the same.

3. middleman -> client        (M2C_Q_R, ~200 B)
       <P2P><M2C_Q_R>
         <reg><ip>...</ip><port>58200</port></reg>
         <relay><ip>...</ip><port>58200</port></relay>
         <log><ip>...</ip><port>58200</port></log>
         <t><ip>...</ip><port>58200</port></t>
       </M2C_Q_R></P2P>
   Different shape from M2D_Q_R: four IpPort fields, no rsp/token/ac.

4. Client -> register:58200   (C2R_C, ~125 B)
       <P2P><C2R_C>
         <uid>9527000XXXXXXXXX</uid>
         <cli><ip>...client LAN IP...</ip><port>...</port></cli>
         <relay><ip>...</ip><port>58200</port></relay>
         <cid>...random i32...</cid>
         <debug>0</debug>
         <family>4</family>
         <p>MAC</p>
         <r>3</r>
       </C2R_C></P2P>
   Short-form UID — see §6.2 for the prefix-match registry lookup.

5. register -> camera         (R2D_C, ~226 B; sent 10 times at ~100 ms)
       <P2P><R2D_C>
         <cli><ip>...client LAN...</ip><port>...</port></cli>
         <cmap><ip>...client public IP...</ip><port>...</port></cmap>
         <relay><ip>...register self...</ip><port>58200</port></relay>
         <sid>...random u32 session id...</sid>
         <cid>...echoed from C2R_C...</cid>
       </R2D_C></P2P>
   Ten fire-and-forget packets. Cameras typically wake within the first
   one or two; the rest are insurance against UDP loss.

6. register -> client         (R2C_C_R, ~200 B; immediately after the
                               first R2D_C is fired)
       <P2P><R2C_C_R>
         <dev><ip>...camera LAN IP...</ip><port>...</port></dev>
         <dmap><ip>...camera LAN IP...</ip><port>...</port></dmap>
         <relay><ip>...register self...</ip><port>58200</port></relay>
         <nat>NULL</nat>
         <sid>...same as R2D_C sid...</sid>
         <rsp>0</rsp>
         <ac>0</ac>
       </R2C_C_R></P2P>

7. register -> client         (R2C_T)
       <P2P><R2C_T>
         <dev>...camera LAN IP...</dev>
         <dmap>...camera LAN IP...</dmap>
         <sid>...</sid>
         <cid>...</cid>
       </R2C_T></P2P>
   No rsp field on R2C_T.

8. Camera -> register:58200   (D2R_C_R, ~150 B; usually 2-3 retransmits
                               at 500 ms / 1 s intervals)
       <P2P><D2R_C_R>
         <sid>...echoed from R2D_C...</sid>
         [<dev>...camera self-address...</dev>]
         <rsp>0</rsp>
       </D2R_C_R></P2P>
   Camera signal that it has received the wake. Log only.

9. Camera comes online on TCP 9000.
   The client (bairelay's discovery / neolink) then performs the regular
   Baichuan TCP login.
```

When the session ends, the camera sends `D2R_DISC` with diagnostic counters; the server replies `R2D_DC_R` with the same `sid` and `rsp = 0`. The camera resumes sleep-mode heartbeats.

## I.6 Field-level subtleties

### I.6.1 Use the UDP source address, not `<dev>`

`D2R_HB` and `D2R_C_R` both carry an inner `<dev><ip>...</ip><port>...</port></dev>` block that the camera populates with its own self-observed LAN address. Do not use this as the destination for replies. NAT'd cameras report a private LAN IP that is not routable from the server's perspective; non-NAT'd cameras have been observed to populate `<dev>` after a stale DHCP cycle. Always reply to the `recv_from` source address (what the kernel observed on the wire). `crates/wake-server/src/register.rs::handle_heartbeat` does this with an explicit comment.

### I.6.2 Long-form vs short-form UIDs

Argus firmware reports a 20-character UID in `D2R_HB` / `D2M_Q` / `D2R_R` — the 16-character UID printed on the camera's sticker plus a 4-character firmware-version suffix. Bairelay's config and `C2R_C` carry the short form. To find the registry entry from a `C2R_C`, do a prefix-match: `stored_uid.starts_with(request_uid)`. Implemented in `CameraRegistry::lookup_fresh`.

### I.6.3 `rsp = -4` is informational

Reolink's actual cloud sends `<rsp>-4</rsp>` in `R2D_R_R`. Cameras proceed to `D2R_HB` regardless. Mirror the cloud — `R2D_R_R { rsp: -4, ac }`. The semantics of `-4` are unknown.

### I.6.4 Empty `<timer/>` and `<retry/>` markers

`M2D_Q_R` includes self-closing `<timer/>` and `<retry/>` elements with no content in any observed capture. Argus firmware does not require non-empty bodies but does require the elements to be present. Modelled as a zero-field `EmptyTag` struct in `crates/core/src/bcudp/xml.rs`; quick-xml renders it as the self-closing form, byte-for-byte with the cloud's reply.

### I.6.5 Random `token` and `ac`; matching is per-session

The cloud generates a fresh `token` (u64) and `ac` (u32) per `D2M_Q`. Subsequent `D2R_R` and `R2D_R_R` echo these. Cameras anchor to the **`ac`** value across the M2D_Q_R → D2R_R → R2D_R_R triangle: replying with a different `ac` in `R2D_R_R` makes the camera silently re-query `D2M_Q`. The `token` does not appear to be checked end-to-end (logged mismatches do not stop the camera from proceeding), but echoing it back faithfully matches the cloud.

`crates/wake-server/src/registry.rs::SessionAnchors` keeps the `(token, ac)` pair under the camera's UID key. The middleman writes it on `D2M_Q`; the register reads it on `D2R_R`. Without that bookkeeping, every `R2D_R_R` would carry a fresh ac and the camera would loop on `D2M_Q` indefinitely.

## I.7 Operator deployment

To make a real Argus battery camera register with the local wake server, the operator must arrange for the camera's DNS lookup of `p2p.reolink.com` (and `p2p1` … `p2p11.reolink.com`) to resolve to the bairelay box's LAN IP. The mechanism is operator-specific; the requirement is just "camera resolves p2p*.reolink.com to the bairelay box".

Common implementations:

- **Pi-hole / AdGuard Home / unbound**: per-domain override redirect to the bairelay box IP.
- **dnsmasq on a router**: `address=/p2p.reolink.com/192.168.x.y` per FQDN.
- **/etc/hosts on the bairelay box**: does **not** work for cameras. The bairelay box's `/etc/hosts` only affects DNS lookups originating from that box. Cameras query whatever DNS server they got from DHCP.

Bairelay's own outgoing discovery (when `discovery = "relay"` in config) goes through the same DNS path — `p2p*.reolink.com:9999` — so once the redirect is in place, both directions land on the local wake server transparently. No bairelay-side DNS configuration knob is required.

### I.7.1 Camera firmware caches DNS aggressively

After changing the DNS redirect, cameras may take a long time (>5 minutes observed; firmware caches in-process beyond the DNS TTL) to pick up the new resolution. The fastest way to flush is `bairelay reboot <camera>`, which forces the camera through its full boot cycle including a fresh DNS lookup. Power-cycling the camera works equally well but takes ~30 s longer. Letting it expire naturally is unreliable.

### I.7.2 Live-verify recipe

`docs/testing.md` § "Local wake server live-verify" captures the full operator-side recipe. Short version: enable `[wake_server]` in config, redirect DNS at the LAN resolver, reboot one camera, watch for `D2M_Q src=<camera> uid=...` in bairelay's log, then trigger an MQTT wake and watch for `R2D_C ->` ten times.

## I.8 Implementation pointers

| Concern                                | Where                                                              |
|----------------------------------------|--------------------------------------------------------------------|
| Wire framing (header + CRC + XOR)      | `crates/core/src/bcudp/{de,ser,crc,xml_crypto}.rs`                 |
| All `UdpXml` variants                  | `crates/core/src/bcudp/xml.rs`                                     |
| Wire round-trip tests                  | `crates/core/src/bcudp/model_tests.rs`                             |
| Listener loops                         | `crates/wake-server/src/{middleman,register}.rs`                   |
| Camera registry + session anchors      | `crates/wake-server/src/registry.rs`                               |
| `advertise_ip` (avoids `0.0.0.0` leak) | `crates/wake-server/src/route.rs`                                  |
| `decode_discovery` / `encode_discovery`| `crates/wake-server/src/packet.rs`                                 |
| Public entrypoints `run` / `run_with_sockets` | `crates/wake-server/src/lib.rs`                             |
| In-process integration tests           | `crates/wake-server/tests/udp_loopback.rs`                         |
| Operator-facing config schema          | `crates/wake-server/src/config.rs`, `sample_config.toml`           |
| Orchestrator integration               | `src/main.rs` (the spawn block right before `orchestrator.run()`)  |
| Live-verify recipe                     | `docs/testing.md` § "Local wake server live-verify"                                       |

## I.9 Open questions

Behaviour not yet observed or characterised:

- The exact rules under which a camera re-anchors (`D2M_Q` mid-session). Cold boot is the only confirmed trigger.
- Whether `<token>` in `D2R_R` is checked at all by the camera, or only by the cloud's bookkeeping. Mismatches are tolerated today.
- The `<r>` value in `D2M_Q` (observed 2 and 3) — meaning unclear; not currently acted on.
- The `<log>` server protocol on port 57850. Cameras emit this address in `M2D_Q_R` but no log connection has been observed during normal operation.
- Cellular-camera variants; contributions welcome.

If any of these become load-bearing, capture against a real cloud and extend the schemas in `crates/core/src/bcudp/xml.rs`.

---

# Part II — motion-push listener

The motion-event-while-disconnected gap. Battery cameras detect motion locally, then notify "the cloud" — and that's it; the only firmware-supported notification path goes outbound, not to the LAN. Reolink's FCM-based push (the path the official mobile app uses) is also dead for third-party clients: Google removed the registration API the prior reverse-engineered integrations depended on. Bairelay closes the gap locally by intercepting the camera's outbound notification at the TCP layer.

Implementation: `src/push_listener.rs`. The TLS-validation gate is reproducible via `tests/scripts/pushx-sink/run.sh`, a self-signed-cert MITM rig that re-runs the handshake against a sleeping camera with `pushx.reolink.com` DNS-hijacked to the rig host (§II.2).

## II.1 What the camera does on motion

A 100 s gateway pcap with bairelay stopped, two operator-triggered motion events, and a single Argus on the LAN nailed down the entire surface:

```
0. Camera detects motion locally.

1. Camera resolves pushx.reolink.com via DNS.
   Cloudflare-fronted; the answer rotates.

2. Camera -> pushx.reolink.com:443  (TCP SYN, then TLS Client Hello with
                                     SNI "pushx.reolink.com")

3. Reolink's cloud completes the TLS handshake and accepts a small POST
   (~432 bytes; per FCM-era reverse-engineering of the same channel by
   prior third-party Reolink integrations, the body is JSON carrying
   `ALMTYPE: MD`, `UID`, `SRVTIME` and friends).

4. Reolink's cloud replies ~540 bytes (a small JSON ack), then FIN.
   Camera teardown.

5. Reolink's backend forwards the alert via Google FCM to whatever
   phone tokens the camera registered via the Baichuan PushInfo command.
```

The connection lasts under one second wall-clock per motion edge. **Nothing else crosses the LAN on motion** — no UDP broadcast, no new BcUdp variant, no fallback. The only steady-state traffic is the regular `D2R_HB` heartbeats every ~10 s to UDP 58200 (Part I), which are unaffected by motion state.

## II.2 Why we don't terminate TLS

We tried. `tests/scripts/pushx-sink/run.sh` stands up `openssl s_server` on `:443` with a self-signed cert (CN + SAN both set to `pushx.reolink.com`); with `pushx.reolink.com` DNS-hijacked to that host, the camera dials in cleanly:

```
1. TCP handshake: completes
2. Camera sends Client Hello with SNI pushx.reolink.com
3. Server replies ServerHello + Certificate (self-signed) + ServerKeyExchange + ServerHelloDone
4. Camera responds: TCP RST, ACK   (~250 ms after Certificate)
```

**A bare TCP RST without a TLS Alert.** That's the fingerprint of an embedded TLS stack (mbedTLS / wolfSSL family) that does cert-chain validation strictly and tears the socket down on failure rather than emitting a polite TLS Alert. We will not be presenting a cert this firmware accepts — the leaf would need to be signed by whatever root Reolink ships in the camera's CA bundle, and that key is on Reolink's side.

If a future firmware ever loosens cert validation, `tests/scripts/pushx-sink/run.sh` is the entry point for re-attempting body decoding.

## II.3 Connect attempt as the signal

But cert validation only blocks the body; the **connection attempt** is plaintext at the TCP layer. And the baseline capture establishes the key invariant: zero connections to `pushx.reolink.com` from the camera over 100 s of idle, and exactly one connection per motion event. So the SYN itself is the motion signal.

What we need is the source IP → camera mapping, and with the wake server running we already have it: its `CameraRegistry` (Part I, §I.6.1) populates this map from `D2R_HB` heartbeats. The camera's TCP source IP for the motion push is the same as its UDP heartbeat source IP (different ephemeral port, same host). `CameraRegistry::lookup_by_ip` does the reverse lookup in one call.

When the registry has no entry (wake server disabled, or no fresh heartbeat), the listener falls back to the camera whose configured `address` is the peer IP. That only needs a literal IP and a stable lease, and it lets an operator hijack `pushx` alone — leaving `p2p*.reolink.com` with Reolink so the official app's remote access keeps working. Two cameras configured on one IP are ambiguous and match neither.

The implementation drops the socket the instant `accept()` returns. There's no TLS termination, no read of bytes, no reply. The camera RSTs us back, then retries — possibly to the real `pushx.reolink.com` if the operator's resolver only redirects on a TTL window, or right back to us if the redirect is sticky. Either way the local motion event has already fired by the time the camera's retry kicks off.

## II.4 End-to-end flow

```
0. Camera is sleeping. Bairelay is past its grace period and disconnected.
   The wake-server's CameraRegistry has the camera's source IP from its
   most recent D2R_HB heartbeat (§I.4 step 6).

1. Camera detects motion. Camera CPU wakes.

2. Camera dials pushx.reolink.com:443 — DNS-hijacked to the bairelay box.

3. push_listener accepts the TCP connection, takes the peer IP.

4. CameraRegistry::lookup_by_ip(peer_ip) -> long-form UID + entry.

5. Linear scan of the camera HashMap finds the CameraHandle whose
   configured (short) UID is a prefix of the registry (long) UID.
   Same trick as `lookup_fresh` (§I.6.2). No registry entry (wake
   server off) -> the camera whose configured `address` is peer_ip.

6. push_listener:
   - publishes status/motion=on to MQTT
   - acquires a wake-lock for `motion_wake_hold_secs` (default 30 s)
   - drops the TCP socket (camera RSTs us back, moves on)

7. The wake-lock acquire kicks the camera handle's connect loop, which
   sends C2R_C to the local wake server (Part I, §I.5). The wake server
   forwards R2D_C to the camera (which is already awake from motion);
   bairelay establishes a Baichuan TCP session.

8. The in-session `motion_listener` (src/camera_tasks.rs) subscribes to
   motion events. The camera reports MotionStatus::Start (motion is
   ongoing) -> publishes status/motion=on (redundant, idempotent),
   plus status/motion/{person,vehicle,animal,other} from the alarm's
   AItype — the first point at which the class is known.
   When motion ends -> MotionStatus::Stop -> publishes status/motion=off
   and every class off.

9. After motion_wake_hold_secs, the push_listener's wake-lock guard
   drops + a fallback status/motion=off (and every class off) is
   published — unless a later push from the same camera arrived
   meanwhile, in which case that push's timer clears. This catches the
   case where step 7 fails (e.g. the camera went back to sleep before
   bairelay reconnected) so HA never gets stuck on motion=on.

10. With no other wake-lock holders, the camera goes idle, the
    watchdog disconnects it, the camera resumes its sleep-mode
    heartbeats.
```

The fallback `motion=off` in step 9 can race with step 8's live `motion=off` in either order; HA dedups. If motion is *still* ongoing when the fallback fires (rare on Argus — most events are sub-30 s bursts), HA sees a brief `off` flicker before the in-session listener's next Stop. Acceptable trade-off — an occasional flicker beats an indefinitely stuck `on`.

## II.5 Operator deployment

To make the listener fire on real motion, three things must be in place:

1. **DNS hijack of `pushx.reolink.com`.** Same mechanism as Part I (§I.7) — operator's LAN resolver overrides the hostname to the bairelay box's IP. Pi-hole / AdGuard / unbound / dnsmasq all support this. Verify with `dig +short pushx.reolink.com` from another LAN host.
2. **A way to match the pushing camera.** Either the wake server (Part I — DNS hijack of `p2p*.reolink.com` plus `[wake_server] enable = true`), whose `D2R_HB` heartbeats populate the registry, or a literal IP `address` on each camera with a DHCP reservation. With neither, every motion connect logs "push from unknown IP — no registry or address match" and bails; config validation refuses that combination up front.
3. **`[push_listener] enable = true`** in `config.toml`. Defaults are sensible for the common case (bind inherits the wake-server bind, port 443, 30 s wake-hold). Override `push_listener_port` only when the bairelay host already runs another HTTPS service. Override `push_listener_addr` only on multi-homed hosts. See `sample_config.toml`.

Bind-permission caveat: port 443 needs root on Linux/macOS (or `CAP_NET_BIND_SERVICE` on Linux). Operators running bairelay as a non-root systemd unit either grant the capability via `AmbientCapabilities=CAP_NET_BIND_SERVICE` or override the port to something un-privileged plus a redirecting iptables / pf rule.

### II.5.1 Live-verify recipe

Same shape as Part I's recipe (`docs/testing.md` § "Local wake server live-verify"). Short version: enable both `[wake_server]` and `[push_listener]`, redirect both DNS names at the LAN resolver, run bairelay (with permission to bind 443), wait for the cameras to drop into idle-sleep, then trigger motion in front of one camera. Watch for `Motion push from camera (treating TCP-accept as motion edge) camera=<name> peer_ip=<ip>` in the bairelay log. The downstream `status/motion=on` MQTT publish + connect-loop reconnect + watchdog disconnect cycle is observable in the same log via `RUST_LOG=bairelay=debug`.

## II.6 Implementation pointers

| Concern                                       | Where                                                                |
|-----------------------------------------------|----------------------------------------------------------------------|
| TCP listener + accept loop                    | `src/push_listener.rs::run`                                          |
| Peer-IP → registry UID                        | `crates/wake-server/src/registry.rs::CameraRegistry::lookup_by_ip`   |
| Long-form UID → camera handle (prefix match)  | `src/push_listener.rs::match_camera_by_uid`                          |
| Peer-IP → configured `address` (fallback)     | `src/push_listener.rs::match_camera_by_address`                      |
| Motion publish + wake-lock + fallback off     | `src/push_listener.rs::fire_motion`                                  |
| Operator-facing config schema                 | `src/config.rs::PushListenerConfig`, `sample_config.toml`            |
| Shared-registry construction                  | `crates/wake-server/src/lib.rs::make_registry`                       |
| Orchestrator integration                      | `src/main.rs` (the spawn block alongside the wake server)            |
| Self-signed cert MITM rig (validation gate)   | `tests/scripts/pushx-sink/run.sh`                                    |

## II.7 Open questions

- **Whether the camera retries elsewhere after the RST.** Within the 100 s baseline window, no fallback traffic was observed — the camera either retries the same hostname (which our hijack catches again) or gives up. Longer captures would clarify.
- **Other cloud endpoints we haven't characterised.** `apis.reolink.com` (device profile), Reolink's own `mqtt-cn-reolink.cn` (China-region MQTT), and any future endpoints could carry alarm-relevant traffic. None observed during motion in the captures so far, but a fuller idle-state capture would map the full set.
- **Whether the camera ever pushes non-motion alarms via this same channel.** Reolink's alarm taxonomy includes `MD`, `PIR`, and `AI` (person/vehicle/animal). Whether all three traverse `pushx.reolink.com` or only `MD` is untested. Today we treat any connect from a registered camera as a motion event regardless; if a non-motion alarm fires the same TCP path, we'd attribute it to motion incorrectly. No symptom observed yet. Either way the push can't carry the class — person / vehicle / animal / other is read from the Baichuan `AlarmEvent` once the push-driven wake reconnects (§II.4 step 8).
- **Cert pinning loosening.** If a future firmware drops to looser TLS validation, `tests/scripts/pushx-sink/` becomes the entry point for body decoding. Re-run that rig on each major firmware bump.
