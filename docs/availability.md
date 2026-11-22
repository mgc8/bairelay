# Availability

Two independent questions get two independent MQTT blocks, because Home
Assistant can only act on what it can tell apart:

- *is the bridge itself up?* → `{prefix}/status`
- *can this camera be talked to?* → `{prefix}/{cam}/presence`

Every discovered entity of this bridge carries both in its `availability` list
(`DiscoveryAvailability`, with the `visibility_topic` each block needs to be
readable from an automation), so a device goes unavailable with its bridge and
with its camera, without a single extra rule in Home Assistant.

## The bridge block

`{prefix}/status` carries `online` while the bridge is connected to the broker.

The word is published **retained** on CONNACK, and `offline` is registered as the
client's **last will** before the connection is even attempted. That ordering is
the whole point: a crash, a killed container, a pulled cable or a Home Assistant
update leaves the broker to publish `offline` by itself. Nothing has to be
running for the bridge to be marked unavailable — which is the only case where
knowing it matters.

## The device block

`{prefix}/{cam}/presence` carries `reachable` or `unreachable`, retained.

**Presence does not follow the session.** A battery doorbell closes its Baichuan
session a few seconds after every wake, by design: the wake lock is released, the
idle timer runs out, the radio goes back to sleep. Mirroring that on availability
would grey out the whole dashboard for most of every day, and the first thing an
operator does with a permanently-grey dashboard is stop trusting it. Sleeping is
not being gone.

Presence is therefore derived from three events that already happen, and adds
**no network call of its own**:

- the bridge starts → `reachable`, optimistically: the camera was configured, and
  the first thing the bridge does is try it;
- a session is opened → `reachable` (a real contact, not a hope);
- a connection attempt **gives up** → `unreachable`.

## Why one failed attempt is enough

A connection attempt to a sleeping camera is not a cheap probe: it is a radio
wake-up. The camera pays for it in battery whether it answers or not, and the
second attempt buys no information the first one did not already give — the same
camera, the same sleep state, the same radio. What a retry would cost is a second
wake for the same answer.

Retrying is not lost either: the wake machinery already runs on its own clock
(the wake lock, the burst guard, the alarm listener), so if something wakes the
camera for a real reason, the next successful session flips presence back to
`reachable` by itself. Presence is a statement about what the bridge has seen,
not a health check the bridge invents.

That is also why publishes are gated on **transitions**: the latch is shared per
camera (`PublishedPresence`, one cell per camera, not per publisher — which is
why only `StatusPublisher::with_presence` may publish presence), and a verdict
that repeats what is already on the wire is dropped before it reaches the broker.
A retained publish is replayed to every subscriber that comes back, so a
duplicated verdict would make every restarting Home Assistant re-fire its
automations for no news.

## What this is for

An automation that stops working silently is worse than one that does not work.
With these two blocks, "no doorbell event for a day" can be told apart:

| bridge | camera | what it means |
| --- | --- | --- |
| `online` | `reachable` | normal |
| `online` | `unreachable` | the camera stopped answering: battery, Wi-Fi, or it was moved |
| `offline` | (last value) | the bridge is down: the camera may be perfectly fine |
| missing | missing | the broker or the add-on is not running at all |

## What stops Home Assistant from accumulating ghost entities

Three mechanisms, and they are not equally strong. Knowing which one is doing the
work matters the next time an entity's identity moves.

**The wire contract is pinned by tests, and that is the real protection.** Every
`unique_id` is built from english slugs — `{prefix}_{camera}_{family}_{leaf}` — and
`object_id` from the same slugs. `topic_names_match_spec`, `each_spec_has_a_unique_id`
and `ai_sensor_gating_by_presence` fail the build if a slug moves. A label is free to
change because it is not part of the identity; a slug is not free, because it is.
`AiObject::topic_name()` and `SettingSpec::unique_id()` are wire surface, not labels:
they mint topics and `unique_id`s. `topic_name()` used to have a sibling
`short_name()` that was the display form; the label now comes from `display_name()`
and the wire form is documented as wire, because a rename sweep that reached
`topic_name()` would silently move `…_ai_animal` and leave the old entity stranded
while its automation stops firing.

**Home Assistant keeps `entity_id` stable.** It fixes the entity id at first
discovery and does not rename it when the `name` field changes. So the switch of
every label to english renames what the owner reads on the dashboard and changes
nothing that an automation refers to. Automations keyed on `entity_id` survive; only
manual dashboards built on entity *names* need a click.

**Discovery publishes retractions, but only within one run.** `publish()` records the
topic set it published per camera and writes an empty retained payload over every
topic that disappears, which covers a known entity whose identity changed while the
process was up. It cannot diff against the previous *process*: that record is
in-memory, so an identity change shipped in a new version is invisible to it. This
is a documented blind spot, not a bug to fix here — no slug moved in the label
switch, so there was nothing to retract, and inventing a retired-identity table
before an identity has actually been retired would be guessing at the schema.

If an identity does move for real, the migration is a manual step: keep publishing
the old `unique_id` for one release, or delete the stale entity in Home Assistant.
The retraction mechanism is a safety net for in-process churn, not a migration tool.
