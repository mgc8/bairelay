# Waking a camera by addressed packet, without the relay

A design note. It records what was measured, what the code guarantees, and what nobody has
established, so that the next reader does not have to re-derive the evening.

## The problem this solves

Cutting the deployment off the vendor cloud and reaching a sleeping battery doorbell are two
goals that pull against each other. The relay path reaches the sleeping camera, and the relay
path is the one that asks twelve hostnames whose fate a DNS outside this project decides (see
"The twelve relay hostnames" in `docs/settings.md`). The local scope asks none of them, and a
battery camera that is asleep does not hear what local sends. So today, locality is bought by
giving up the wake, and the wake is kept by keeping the question outside the house.

## What the two paths actually put on the wire

They are not two flavours of one packet, which is the assumption that made this hard to read.

The working path sends `R2D_C`: **a wake command, addressed to the source address the camera
itself used when it came to register with us.** The address is not configured and not
guessed; it is the UDP source address of a live registry entry, written by the camera's own
`D2R_HB` beats. The client is answered with a session and a relay description, and the burst
goes out before the answer so the camera is likely awake by the time TCP is attempted.

The local discovery path sends `C2D_C`: **a client-initiated proposal**, expecting a `D2C_CR`
answer. It carries the uid and a client id. Our side requires nothing else of the exchange -
no token, no registration reference, no nonce, no paired key - and it consults no store of
ours at all: neither the registry nor any registration table.

That asymmetry is the whole finding. The path that works does not ask the camera's permission;
it tells a camera that already told us where it is. The path that failed asked, correctly, to
an address that was right, on three ports, while the camera was awake and talking to us - and
we have **never captured a real `D2C_CR`**. Every round trip in this tree is against a peer we
script ourselves. Whether a Baichuan battery doorbell answers `C2D_C` at all is therefore not
known, and is not settled by anything in this repository.

## The rule for sending an addressed wake outside the relay arm

We emit `R2D_C` only when three conditions hold together:

1. the destination is the address attached to a **live** registry entry, so a camera evicted
   by `stale_after_ms` cannot be sent anything;
2. that entry's uid belongs to a camera **declared in the configuration**, so nothing we were
   never told about receives a command;
3. it is sent **because something needs it** - a client asking for that camera - never on a
   timer.

No broadcast, no guessed address, no address that is not in the registry, no periodic wake.

The justification is in the mechanism rather than in intent: the camera came to us unprompted
and told us where it is, and we answer it there.

Scope is bounded by those three conditions and by nothing else, and that has to be said
plainly because it is not a hedge: on the wire our `R2D_C` and a real server's are
indistinguishable. If the camera believes its cloud is this bridge - which is the situation of
any installation where it registers and beats here - then a wake sent under these conditions
is indistinguishable from one the camera expects. The three conditions are what keep that
capability narrow, and code review should treat any widening of them as the change of scope
that it is.

#### The startup cycle is not a timer

The bridge does put wake packets on the wire before any client has asked for anything: once, at
module start, when a camera named in the configuration is not reachable yet. That cycle stays by
the operator's decision, recorded as a request and not an oversight - warming the buffers and
trying to reach the camera at start costs the installation nothing it has not agreed to, and it
buys a session that is already warm when the first real need arrives.

It is not an exception to condition three, because it was never a timer. Measured on the
installation: four attempts, spaced two, four and eight seconds, then silence - a bounded cycle
that ends, not a cadence that returns. The journal shows the same shape, `Trying TCP discovery`
then `Trying local discovery`, four times, and nothing more until something asks. A reader who
takes "never on a timer" as a claim about all wake traffic has read it too broadly: the rule
forbids a *recurring* order, and what runs at start is one demand the bridge makes once, for, and then stops making.

The frame that goes out at start is the **client-initiated `C2D_C` proposal**, not an `R2D_C`
order: it announces this bridge to an address it already has, and it consults no registry - the
registry is only ever read by the relay arm, which answers a client's request for a camera. A
reader who takes the startup cycle for an `R2D_C` will look for a registry lookup that is not
there, and will find none in the code either.
own account, and then stops making.

## What startup discovery changes here, and what it does not

`[startup_discovery]` (see “Cameras discovered at startup” in `docs/settings.md`) lets a camera
enter the running configuration without a human typing its uid. Read against the three
conditions above, it changes none of them and one assumption underneath them, and the
assumption is the part a reviewer should be told about rather than left to notice.

The conditions stand as written. The scan emits no `R2D_C`: it sends one client-side `C2D_S`
broadcast — a request, not an order — and it never sends anything to an address it did not
receive a packet from. A camera it adopts is appended to `config.cameras` before the wake
server, the orchestrator and the MQTT payloads are built, so from the first wake question
onwards it is an ordinary declared camera: condition 2 is satisfied by the same list it always
consulted, and a camera the scan did not adopt and nobody declared still gets the explicit
refusal, not an invented address.

What changed is where an entry in that list can come from. It used to be that condition 2 could
be read as "a person wrote this identifier down, from the label on the device". Now it can also
mean "a device said this about itself, once, in answer to a broadcast, and the bridge believed
it". On a LAN where a camera can be added by whoever plugs it in, that is a wider door than the
one this note describes, and the mitigation that exists is the one the mode is built on:
adoption needs credentials the operator wrote for exactly that purpose, so a camera that wanders
onto the LAN is heard, named in a warning by its uid tail, and not connected to. Adoption also
never overrides a declaration, so the operator's file remains the description of the installation
wherever the two disagree.

The scan's own silence is worth keeping in view while reviewing it. It is the first time this
code sends `C2D_S`; the packet is in the protocol model, documented there as discovery of *any*
client, and it has never been sent by anything in this tree — `grep C2D_S` reaches tests only.
The request this project does send, `C2D_C`, is not an alternative worth switching to, because
its reply `D2C_C_R` carries no uid at all: a camera answering it could not say who it was even
if it wanted to. So the mode ships with a bounded window and a warning when nothing answers, and
claims nothing about what a real firmware does with the request. A run of this scan against a
camera that answers would be the first measurement of it, and belongs in this note.

## What our own server already answers

The bridge can already be the relay those twelve names point at, with no new code:

- a camera registering is stored under the uid it sent, with its source address;
- a client asking "where is this uid" is answered from that store: hit and the burst fires at
  the recorded address, miss and the answer is an explicit refusal (`rsp: -1`, no relay), not
  a silence.

So locality by resolver - pointing the twelve names at this machine - is a deployment change,
not a build change. It inherits the registry's freshness limit: a camera that has not beaten
within `stale_after_ms` is not in the store and gets nothing back.

## Named gaps

These are open, and each is written here rather than smoothed over.

- **Encoder behaviour on an over-long uid.** The binary header's uid field is fixed width, and
  the decoder's assertions pin it at 16 and 32 bytes; a long configured form and the short
  wire form are the same value in two envelopes, and the truncation is demonstrated **on
  receipt**. What the encoder does with a value longer than the field - truncate, reject, or
  pad - is not established. One day that should be read and written down.
- **The registry's prefix fallback is one-way.** Lookup tries an exact match, then falls back
  to any stored uid that *starts with* the requested uid, which lets a short-form client find a
  long-form registration. The reverse does not work: asking with the long form when the store
  holds the short one misses. The push listener already matches in both directions; the
  registry does not, and that is a code item of its own. Its companion, same shape of problem: the beat-window table is
keyed by the uid exactly as the beat carried it, so a camera that ever presented both forms
would split its window in two - two reports, each internally correct and neither complete.
Named here rather than fixed by fusing keys tonight, because both turn on the same question:
which form of a uid counts as the same camera.
- **No real `D2C_CR` exists in the corpus**, so the failure of the local proposal cannot be
  attributed to identity, to timing, or to firmware indifference. Anything said about why it
  went unanswered is a hypothesis.
- **Whether an addressed wake reaches a sleeping camera that is registered elsewhere** is not
  measured, and this note does not claim it. The rule above bounds what we would send; it does
  not describe what a camera would accept.
