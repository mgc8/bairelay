# Camera settings

The settings engine reads and writes the camera's own setting families over the
Baichuan TCP session, mirrors them onto MQTT, and offers them to Home Assistant
as discovered entities. It exists because a battery doorbell answers for a few
seconds at a time: everything here is organised around spending that window
well.

The measurements come from the operator's private protocol notebooks, which are
**not published with this repository**: they carry the device identifier in clear
text, the camera's administrator account, broker credentials and the home's public
host name, and this camera is off the cloud precisely so that it cannot be found.
A cleaned-up copy is not the fix — a notebook that has held a credential is not
something to publish in a hurry. So the citations in the code say where a thing was
measured rather than pointing at a file nobody outside the house can open, and
wherever a measurement decides a behaviour, the number is restated here and in the
comment that depends on it. This file is enough to review the behaviour.

## Families

A family is one block of camera settings with its own read/write message ids and
its own topic segment. `Family::slug()` is the topic segment; the same slugs are
what `query/settings` accepts.

| slug | block | read / write | writable |
| --- | --- | --- | --- |
| `audio` | `<audioCfg>` | 264 / 265 | yes |
| `denoise` | `<aiDenoise>` | 439 / 440 | yes |
| `image` | `<VideoInput>` + `<InputAdvanceCfg>` | 26 / 25 | yes |
| `osd` | `<OsdDatetime>` + `<OsdChannelName>` | 44 / 45 | partly (see below) |
| `motion` | `<MD>` / `<MdAlarm>` | 46 / 47 | partly (see below) |
| `pir` | `<RfAlarmCfg>` | 212 / 213 | yes |
| `ntp` | `<Ntp>` | 38 / 39 | yes |
| `dst` | `<Dst>` | 106 / 107 | yes |
| `autoreboot` | `<AutoReboot>` | 101 / 100 | yes |
| `general` | `<SystemGeneral>` | 104 / 105 | yes |
| `chime` | paired chime | 484, 485, 486, 487 read · 483, 485, 486, 610 write | yes |
| `mask` | `<Shelter>` | 52 | no |
| `sleep` | `<sleepState>` | 574 | no |
| `wifi` | `<WifiSignal>` | 115 | no |
| `battery` | `<BatteryInfo>` | 253 | no, and never worth a wake |
| `ai` | `<AiDetectCfg>` | 342 / 343 | pinned read-only (see below) |

### Why some families are read-only

These are not gaps in the UI; each one is a camera behaviour that was measured,
and the bridge publishes a reason next to the entity so the operator is never
left with a silent switch:

- **`motion`** — the getter (46) answers with `<MdAlarm>`, a flat shape the
  setter (47) rejects. Writing it back would be a guaranteed refusal, so the
  bridge refuses locally and says so (`status/motion/reason`). `<MD>` answers
  47 normally, so a camera that gives `<MD>` keeps its switches.
- **`osd`** — the name and the watermark live in `<OsdChannelName>`, which the
  read does not return. A write would have to invent the rest of that block.
- **`mask`, `sleep`, `wifi`** — one-way blocks: the camera prints them and
  accepts no equivalent write (`<Shelter>` has no setter on this firmware).
- **`battery`** — readable, and deliberately never read to *buy* a wake: 253 is
  answered only when the camera is already up, so asking for it first would pay
  a wake-up for a number.
- **`ai`** — the per-type detection settings (342/343) are asked one type at a
  time and the enable tag has never been seen in a 342 dump from this firmware
  generation. The family is pinned read-only, with a reason, rather than
  publishing switches that cannot be told from ones that work.

The verdict of every read is one of four things (`Verdict`): **writable** (the
block came back and we can send it back), **not writable** (structural proof we
cannot send it back — this is the bridge refusing, not the camera), **transient**
(a `400`, a timeout, an unusable block: proves nothing, so the switches stay as
they were and the reason is published next to them), or **unknown** (nothing was
even attempted: no session, no capability — discovery changes nothing, the reason
is still published).

That distinction is the point of the design. A sleeping firmware answers `400`
to video blocks and gives them back twenty seconds later; treating that as "not
writable" would strip working controls from Home Assistant until the next
restart.

## Topics

| topic | direction | meaning |
| --- | --- | --- |
| `{prefix}/{cam}/control/{family}/{leaf}` | in | one value, as the camera would print it |
| `{prefix}/{cam}/status/{family}/{leaf}` | out | the value as read back |
| `{prefix}/{cam}/status/{family}/json` | out | the whole block, camera spellings |
| `{prefix}/{cam}/status/{family}/reason` | out | why a family is read-only, or why a read failed |
| `{prefix}/{cam}/query/settings` | in | re-read every family (one wake window) |
| `{prefix}/{cam}/query/settings/{family}` | in | re-read one family |
| `{prefix}/{cam}/status/burst` | out | what the last read burst did, as JSON |

A control message is validated **before** anything is sent: an out-of-range
volume, an unknown leaf, a text longer than the camera accepts, an NTP server
that is not a host, are all refused on the spot, and the refusal is answered with
the reason instead of silence. `FAIL` on the command topic means the camera said
no; a local refusal never reaches it.

## The read burst

`src/burst.rs`. A doorbell press or a motion edge is the only moment a sleeping
camera is reachable, so the reads are scheduled into that window rather than
asked whenever someone wants an answer.

Rules, and what each one is for:

- **one burst at a time**, with a `GUARD_WINDOW` of 20 s: a visitor press, the
  webhook push and the alarm event arrive inside the same half-second. Without
  the window the bridge pays two full read rounds for one finger on the button.
- **a budget, not a wish list**: `COMMAND_BUDGET` = 8 commands, `TIME_BUDGET`
  = 25 s. The real limit is the camera — measured `status/burst` runs show four
  of eight commands refused with a session of about fifteen seconds — so the
  plan is ordered by what the camera answers in time, not by the catalogue:
  `motion` and `osd` first (the two families that answer every day, in the frame
  shape this firmware accepts), then the rest cheapest-first, `image` last (its
  26 slot answers `<Shelter>` instead of `<VideoInput>`, which costs a retry).
- **a family that refuses three wakes in a row drops out of the plan**, and two
  refusals in a row end the burst. The counts live in memory on purpose: the
  durable question ("is this family writable") is the state file's job, while
  "am I still paying for a read this camera keeps refusing" is a per-session
  question, and a restart reopening the three attempts is exactly what a restart
  should mean.
- **battery is asked last**, and only after it has answered enough times to be
  worth the command.
- the burst publishes `status/burst` with the plan, the families read, the ones
  skipped and the reason it stopped.
- **the startup read is not a wake.** `src/startup_wake.rs` calls the burst with
  reason `startup`, and only for the families the state file has never proved. The
  startup wake already paid for a session to warm the last-frame buffer, so the read
  spends commands inside that session and opens no wake of its own. With every
  family already proved it triggers nothing at all: an ordinary restart costs zero
  commands. The guard window still applies, so a press landing in the 20 s after a
  startup read is published immediately and read on the next wake.

`Wake` is what started it — `ButtonPress`, `Motion` or `Push` — and appears in
that status payload.

## Which families may be written: the state file

Discovery marks a family's entities writable only when a read has proved it. On
restart the bridge would otherwise have to re-read every family to know that, so
the proven families are kept next to the config file, in
`config-families-read.json`:

```json
{"version":1,"cameras":{"Doorbell":["audio","osd","motion"]}}
```

Slugs only — no values, no identifiers, nothing to protect. A missing,
unreadable or future-version file is treated as "nothing proved yet", which is
the safe direction: entities come back read-only until the next read, and the
bridge never starts refusing to boot because of a scratch file.

The migration renamed this file (it was `config-familles-lues.json`). The old name
is therefore still read - once, and only when the new name is absent - because the
rename moved the name and not the proof: an installation that had proved twenty
families woke up with none of them, and every write entity went read-only for a
reason the camera could not be asked about. The condition is narrow on purpose. Once
`config-families-read.json` exists it is the authority alone: a family withdrawn
from it (saving an empty list removes the camera entry) must not come back through
the old name, or the bridge would promise a write on a proof it had just taken away.
Whatever was found under the old name is rewritten under the new one at the next
change, and the old file is left where it is.

## Settings one-shots

For a camera in front of you, without MQTT:

```
bairelay audio-settings -c config.toml Porche --volume 60 --talk 45
bairelay chime-settings -c config.toml Porche --volume 3 --melody 5 --ring
```

Both read first, rewrite only what you named, and print the values the camera
answered afterwards — the camera's answer, not the value you asked for. They are
the renamed `reglages-audio` / `reglages-carillon`; see the migration table.

## Stream selection

`stream` in a camera block (`none | all | main | sub | extern`, default `all`)
decides which RTSP streams **that camera may serve**. The URL suffix picks what
a client *asks for*: `/cam`, `/cam/main`, `/cam/sub`, `/cam/extern`
(case-insensitive; `mainstream`, `substream`, `externstream` also parse),
resolved in `crates/rtsp/src/url.rs` and used at
`crates/rtsp/src/server/connection.rs`. The option and the suffix are two
different questions, and the option is the one that used to be ignored: a camera
set to `stream = "sub"` woke its radio and served the main stream to anyone who
typed `/main`.

- `all` (default) answers every stream — nothing changes for a configuration
  that never mentioned the option;
- `main`, `sub`, `extern` answer that stream only;
- `none` answers none, for a camera that should never be watched over RTSP;
- a refusal happens **before the wake lock is taken**: a request the camera will
  not serve cannot cost a sleeping camera a wake-up, nor hold a reference that
  keeps it awake while it is being refused.

`extern` is a request for a balanced stream, and the RTSP server retries an
unavailable `extern` as `sub`. So with `stream = "sub"` a `/extern` client is
served the substream through that retry — the refusal is what routes it there.
With `stream = "main"` both halves are refused and the client gets the error,
naming the option both times.

On a battery doorbell this is a battery option, not a picture-quality option: the
main stream is a radio wake-up, a full-bitrate session and a held wake lock. A
setup that only ever shows the substream in Home Assistant should say
`stream = "sub"`, and then a stray `/main` from an old dashboard costs nothing
instead of costing a wake.

## Renaming

Everything the bridge **shows** is English now: Home Assistant entity names, the
reason sentences on `status/{family}/reason`, and the values on `status/burst`.
Everything Home Assistant **keys on** is untouched: `unique_id`, `object_id`, the
config topics, the state topics and the state payloads.

That distinction is the whole migration. Home Assistant assigns an `entity_id`
once, when it first sees a discovery record, and never renames it when the
`name` field later changes. So his automations that reference
`number.bairelay_frontdoor_volume_visiteur` keep working exactly as written; what
changes is the label next to it in the dashboard. The two things that would have
broken them — a changed `unique_id`, or a retained config left behind on an old
topic — did not happen here, and `topic_names_match_spec` plus
`chaque_spec_a_un_unique_id_unique` are what keep them from happening later.

### Entity names

| was | now |
| --- | --- |
| Volume général | Speaker volume |
| Volume interphone (parole) | Intercom volume (talk) |
| Volume visiteur | Visitor volume |
| Haut-parleur visiteur | Visitor loudspeaker |
| Débruitage micro | Microphone denoise |
| Force débruitage | Force denoise |
| Luminosité | Brightness |
| Contraste | Contrast |
| Saturation | Saturation |
| Teinte | Hue |
| Netteté | Sharpness |
| Mode jour/nuit | Day/night mode |
| Horodatage à l'écran | On-screen timestamp |
| Nom à l'écran | On-screen name |
| Texte du nom à l'écran | On-screen name text |
| Filigrane | Watermark |
| Détection de mouvement | Motion detection |
| Sensibilité mouvement | Motion sensitivity |
| Sensibilité PIR | PIR sensitivity |
| Intervalle entre deux déclenchements PIR | PIR trigger interval |
| Synchro NTP | NTP sync |
| Serveur NTP | NTP server |
| Intervalle de synchro | Sync interval |
| Heure d'été | Daylight saving |
| Décalage heure d'été | Daylight saving offset |
| Redémarrage automatique | Auto reboot |
| Jour de redémarrage | Reboot day |
| Heure de redémarrage | Reboot hour |
| Minute de redémarrage | Reboot minute |
| Format de date | Date format |
| Format heure (0=24h, 1=12h) | Time format (0=24h, 1=12h) |
| Langue | Language |
| Nom système de l'appareil | Device system name |
| En heure d'été actuellement | Currently in daylight saving |
| Masquage actif | Masking active |
| Emplacements de masque | Mask slots |
| En veille | Sleeping |
| Signal Wi-Fi | Wi-Fi signal |
| Tension batterie | Battery voltage |
| Courant batterie | Battery current |
| Température batterie | Battery temperature |
| État de charge | Charge status |
| Batterie faible | Battery low |
| Carillons appairés | Paired chimes |
| Volume du carillon | Chime volume |
| Mélodie du carillon | Chime melody |
| Mélodies déclarées par la caméra | Camera-declared melodies |
| Faire sonner le carillon | Ring the chime |
| Horaires du projecteur | Floodlight schedule |
| Projecteur | Floodlight |
| Mouvement détecté | Motion detected |
| Appui sur le bouton | Button press |
| Voyant lumineux | Status light |
| Mode infrarouge | Infrared mode |
| Détection PIR | PIR detection |
| Niveau de batterie | Battery level |
| Aperçu | Preview |
| Redémarrer | Reboot |
| Sirène | Siren |
| Préréglage | Preset |
| PTZ gauche / droite / haut / bas | PTZ Left / PTZ Right / PTZ Up / PTZ Down |
| Reconnaissance personne / véhicule / animal / figure / colis | Person detection / Vehicle detection / Animal detection / Face detection / Package detection |

### Reason sentences on `status/{family}/reason`

Non-retained, ephemeral, nothing keys on them — they are for the human reading
the log or the entity's last state.

| was | now |
| --- | --- |
| la caméra n'a pas répondu à temps | the camera did not answer in time |
| la caméra lit ce réglage mais refuse qu'on lui renvoie le bloc : lecture seule | the camera reads this setting but refuses the block back: read-only |
| aucune session Baïchuan en cours : rien n'a été envoyé à la caméra | no Baichuan session is open: nothing was sent to the camera |
| — réglage non verrouillé : la famille garde son état | `- setting not locked: the family keeps its state` (a suffix, published after the technical reason) |
| erreur de lecture transitoire : la famille reste publiée modifiable | transient read error: the family stays published as writable |
| la réponse de la caméra ne contient pas `<OsdChannelName>` : rien à lui renvoyer, lecture seule | the camera's answer carries no `<OsdChannelName>`: nothing to send back, read-only |
| 484 a répondu sans aucune entrée reconnue : carillon non énuméré, … | 484 answered with nothing recognised: chime not enumerated, … |
| la caméra n'a pas répondu à 484 : aucun identifiant de carillon, … | the camera did not answer 484: no chime id, … |
| aucune balise d'activation connue par type (342/AiDetectCfg ne documente que … | no known enable tag for this type (342/AiDetectCfg documents only … |
| la caméra n'a donné aucun motif | the camera gave no reason |
| motif masqué : il aurait pu contenir un secret | reason hidden: it could have contained a secret |

The credential mask still matches the French words `mot de passe` — that list is
a scanner over the camera's text, not a label, and translating it would blind the
mask.

### `status/burst` payload

The keys changed too, because the topic is new on this branch and nothing
consumes it yet — cheaper to fix now than after someone builds a dashboard on it.

```json
{"raison":"appui","etat":"terminee","commandes":3,"budget":8,"familles":2}
{"reason":"press","state":"done","commands":3,"budget":8,"families":2}
```

| key | now |
| --- | --- |
| raison | reason — `appui`→`press`, `mouvement`→`motion`, `poussee`→`push` |
| etat | state — `sans-mqtt`→`no-mqtt`, `flux`→`stream`, `sans-session`→`no-session`, `refus`→`refused`, `budget`→`budget`, `duree`→`duration`, `annulee`→`cancelled`, `terminee`→`done` |
| commandes | commands |
| familles | families |

### Ghost entities, and what still guards against them

When an `object_id` really does change, Home Assistant publishes the new entity
and keeps the old one until an empty retained payload arrives on the old config
topic. The bridge already does that diff: `publish()` records the topic set it
published per camera and clears every topic that disappeared from it
(`publish_diffs_against_previous_set_clears_disappearing_topics`,
`une_famille_relue_se_publie_vide_avant_sa_nouvelle_configuration`), which covers
a *known* entity whose identity changes — not only an unknown camera.

The one thing that mechanism cannot see is a change made **between two runs**:
the recorded set lives in memory, so a restart never diffs against what the
previous version published. Nothing needs that today — no slug moved — but this
pass nearly needed it: the label pass first rewrote `AiObject::Animal =>
"animal"` inside `topic_name()`, which is the topic and `unique_id` suffix, not
the label, and `topic_names_match_spec` failed immediately. So the protection is
the pinned wire contract, not the cleanup: `topic_name()` now carries a doc line
saying it is wire surface, and those tests are what stand between a rename and
two hundred entities his dashboard no longer finds.


## Wake announcements

A battery camera pushes a small UDP packet on the registration port when it wakes, and
again when an activity ends. The bridge decodes it (message type `0x31`, in
`crates/core/src/bcudp/announcement.rs`), logs it, and acts on three of its values:

| what the packet says | what the bridge does |
| --- | --- |
| `event` 3 — the camera is awake | one guarded read of that camera, when `read_on_wake` is on |
| `event` 4 with `reason` 19, `duration` under 30 s (or none) — a button press ended | publish the press, and read nothing |
| `event` 4 with `reason` 19 and a longer `duration` — the activity the button started is over | log it, and stop there |
| anything else | log it, and stop there |

`duration` is what separates the two `reason` 19 rows, and it is not a judgement call.
`event` 4 closes an activity and `reason` 19 names the button as its cause, which reads
like a release — until the camera sends the same pair at the end of the wake session that
button opened. Measured on this installation: the announcement at 17:31:00 carried
`duration=Some(119449)` against a wake that opened at 17:29:01, so its activity was the
session, and publishing a press for it rang the operator's phone for a visitor who had
left two minutes earlier. The longest release ever captured off this firmware is 19 281 ms
(capture F in `crates/core/src/bcudp/announcement.rs`), so the bound sits at 30 s: above
every release measured, far below a session end. When the bound refuses a pair, the log
says so — `button activity outlasted the press window` — with the duration it refused.

The press goes out through the same function the session's alarm stream and the webhook
use, so the dedupe window in `src/status_cache.rs` decides which source speaks: one
press, whichever got there first, on `{prefix}/{camera}/status/doorbell`, payload `press`,
not retained. It is the only press path that needs neither a session nor the address
registry, so it keeps working in the seconds after the module restarts, when the
registry — which lives in memory — is empty.

Three settings per camera, one rule each:

| key | default | what it decides |
| --- | --- | --- |
| `read_on_wake` | `true` | whether this camera's announcements are acted on at all |
| `read_hold_secs` | `20` | how long the bridge waits for the session the announcement is meant to open |
| `read_interval_secs` | `60` | minimum spacing between two reads **that got a session** |

`read_on_wake` defaults to on because the announcement arrives when the camera is already
awake — that is what it is announcing. Its cost is arithmetic: one session per announced
wake, so a camera that wakes thirty times a day is thirty sessions a day, and a camera
that wakes twice is two. **The number of wakes per day is not measured for this
installation's camera, and neither is the share of them that happen at night**: until it
is, the cost of leaving this on is stated as a formula and not as a figure. Read the log
of one ordinary day, count the announcement lines, and the formula becomes a number. What it buys is a session at that moment instead of
at the end of whatever the camera woke for, so a wake that carried no press still leaves
the battery level and the settings current. What it costs is one session per announced
wake, including the detections that carry nothing, which is why it can be switched off per
camera. It is not the way to learn about a press sooner: that path needs no session.

A read that got no session is not charged to `read_interval_secs`, because it cost
nothing. Nothing on this path holds a camera awake longer than the guard window: the wake
lock is released as soon as the session appears, and a camera that does not answer is
released at the end of `read_hold_secs`.

### What the announcements are not published as

The raw JSON of an announcement is **not** published to MQTT, and there is no topic for
it. Nothing on this installation consumes it: the automations listen for the press event,
not for the announcement. The packet stays where it is useful — in the log, with `event`,
`reason` and the durations — which is also where an unexpected value gets answered. A
topic nobody listens to is a contract nobody keeps, so the topic helper that had been
written for it is gone rather than kept as an option.

### `discovery = "none"`

One camera may be told to emit nothing on the discovery port. That variant exists to stop
broadcasting when a camera is reachable by a known address; it is not a way to look less
far, and it is not the default. The default stays `relay` for every configuration that does
not name a scope, and a test in `src/config.rs` pins exactly that, so the variant cannot
 quietly become the behaviour of a file that never asked for it.

### The twelve relay hostnames

A camera whose configuration names a `uid` and whose discovery scope is above `local` has
that uid handed to twelve hostnames on every lookup. This is the sentence the bridge logs
once per camera at startup, and under `check-config`, quoted here rather than paraphrased so
the journal and this page cannot disagree:

```
camera uid is sent outside this network: a lookup asks the twelve relay hostnames
(p2p.reolink.com and p2p1 through p2p11) where this uid is, and the same question goes to
each of them because the lookup does not stop at the first answer. Which of those names land
on this LAN is decided by the network's DNS, not by this project: check that list before
describing this deployment as offline. `local` asks none of them, but without an IP-literal
`address` it broadcasts only, which a sleeping battery camera cannot hear
```

Three things follow, and none of them is a setting.

**The lookup does not stop at the first answer.** Every one of the twelve names is resolved
and asked, and each receives the same question about the same uid. A relay that answers first
does not suppress the other eleven.

**Where those twelve go is not this project's decision.** They are resolved by the network's
resolver. A deployment is local to the extent that the resolver answers those names with a
machine inside the network, and no `discovery` value changes that list. `relay` asks the
twelve; `local` asks none. Nothing in between asks fewer than twelve.

**`local` is the only scope that keeps the uid at home, and it is not free.** Without an
`address` written as an IP literal it has no destination of its own and falls back to
broadcast, which a battery doorbell that is asleep does not hear. The scope and the address
are one decision, not two.

#### The list, and how to check it

The names come from the client (`p2p.reolink.com`, then `p2p1` through `p2p11`). This loop
prints what your own resolver makes of each, plus the two names the account path uses:

```sh
for h in p2p.reolink.com p2p1.reolink.com p2p2.reolink.com p2p3.reolink.com p2p4.reolink.com \
         p2p5.reolink.com p2p6.reolink.com p2p7.reolink.com p2p8.reolink.com p2p9.reolink.com \
         p2p10.reolink.com p2p11.reolink.com apis.reolink.com reolink.com; do
	printf '%-18s %s\n' "$h" "$(getent ahostsv4 "$h" | awk '{print $1; exit}')"
done
```

`getent` asks the resolver the machine is configured to use, which is the question that
matters for a deployment. To ask one named server instead, substitute
`nslookup "$h" 192.0.2.53` for the `getent` call.

Recorded on one installation on 2026-11-14 at 22:35 (Europe/Paris), against that network's
own resolver. Read it as one evening's measurement of one network, not as a property of this
software: the point of the table is that it can be re-run and brought back to one column.

| Name | Answer |
| --- | --- |
| `reolink.com` | the in-network host |
| `p2p.reolink.com` | the in-network host |
| `p2p1.reolink.com` | the in-network host |
| `p2p2.reolink.com` | the in-network host |
| `p2p3.reolink.com` | the in-network host |
| `p2p4.reolink.com` | the in-network host |
| `p2p5.reolink.com` | the in-network host |
| `p2p6.reolink.com` | `129.159.158.146` |
| `p2p7.reolink.com` | `15.188.197.53` |
| `p2p8.reolink.com` | `52.56.65.139` |
| `p2p9.reolink.com` | `3.151.10.72` |
| `p2p10.reolink.com` | `52.70.82.252` |
| `p2p11.reolink.com` | `184.34.72.185` |
| `apis.reolink.com` | `104.18.24.250`, `104.18.25.250` |

Counting that table: **six of the twelve relay names answer inside the network** (`p2p`,
`p2p1` through `p2p5`) and **six answer outside it** (`p2p6` through `p2p11`). Outside the
relay list, the root domain answers inside the network and `apis.reolink.com` answers with
two public addresses. So on that evening, every lookup asked the same uid of six servers this
network does not control.

One honest note stays. The addresses on the right are whatever that resolver returned that
evening for public names: they characterise the network's view, not this software and not the
camera, and they will differ elsewhere - which is exactly why the loop above, and not this
table, is what to re-run after a resolver change.

A deployment whose twelve answers all name a machine inside the network never asks a server
outside it, whatever `discovery` says. A deployment where one name resolves outward sends the
camera's uid to somebody else's server on every lookup, and `discovery = "relay"` makes that
happen twelve times over. The fix is the resolver, which lives outside this project; the
in-project consequence is documented under `discovery` and in the wake-server design note.

## The registration beats

The camera beats to the cloud it was told about, which on this bridge is us. Two settings,
deliberately unrelated:

- `hb_advice_ms` in `[wake_server]` is **what we ask for**: the tempo put in the
  registration reply.
- `stale_after_ms` is **what we tolerate**: how old a registry entry may get before its
  address stops being used to match an incoming connection.

There is one rule between them, and knowing it before touching either one is the
difference between a configuration and an evening lost: startup **refuses** a
`stale_after_ms` shorter than `heartbeat_ms`. What that refusal protects is not a number,
it is a camera. A freshness window shorter than the beat evicts a living camera from the
registry, because the only thing keeping its address alive is the beat. An evicted camera
has no known address, so the connection it makes when it wakes cannot be matched to it:
the push is discarded, and a camera that is already awake is never read. That is a silent
failure with a loud cause.

What is *not* checked is the advice, and that is deliberate. `hb_advice_ms` is a request we
send; the camera answers it or ignores it, and nothing here knows which. Measured here, a
camera beats every ten seconds while the advice says twenty, so this bridge is already
ignored on that number — which is exactly why tying the window to the advice would refuse
configurations that are healthy. It has been tried in the field the other way round: an
advice of 300 000 ms set beside a window of 80 000 ms, and the binary refused to start,
which took the module down for ten minutes over a number the firmware never reads. So the
advice is **warned about and never refused** (`advice_outlives_the_window`), the window is
checked against the beat tempo, and the advice stays what it is: a request whose effect is
unproven, not a lever. If the experiment below shows the camera does follow the advice, the
warning becomes information rather than a contradiction — but the refusal stays where it
is, because it guards the window, not the advice.

### Procedure: does a longer advice change anything

A long advice has never been tested. Worth one evening, as a measurement and not as a
promise:

1. set `hb_advice_ms = 300000` in `[wake_server]` and restart the module;
2. read the interval between consecutive beats of each camera from the log for ten
   minutes;
3. if the interval moved, the advice is a lever — and press detection must stay
   independent of the beat, which the announcement path already is;
4. if it did not move, the tempo is a constant of the firmware and `hb_advice_ms` only
   records what was asked. Write that here, so nobody spends another evening believing the
   setting drives the radio.

B
o
t
h
 
o
u
t
c
o
m
e
s
 
a
r
e
 
s
t
i
l
l
 
o
p
e
n
,
 
a
n
d
 
n
e
i
t
h
e
r
 
i
s
 
b
e
i
n
g
 
m
e
a
s
u
r
e
d
 
r
i
g
h
t
 
n
o
w
:
 
u
n
d
e
r
 
t
h
e
 
r
u
l
e


a
b
o
v
e
,
 
a
n
 
a
d
v
i
c
e
 
l
o
n
g
e
r
 
t
h
a
n
 
t
h
e
 
f
r
e
s
h
n
e
s
s
 
w
i
n
d
o
w
 
c
a
n
n
o
t
 
e
v
e
n
 
s
t
a
r
t
,
 
s
o
 
a
 
f
i
v
e
-
m
i
n
u
t
e


a
d
v
i
c
e
 
b
e
s
i
d
e
 
a
n
 
8
0
 
0
0
0
 
m
s
 
w
i
n
d
o
w
 
i
s
 
r
e
f
u
s
e
d
 
b
y
 
t
h
e
 
b
i
n
a
r
y
 
r
a
t
h
e
r
 
t
h
a
n
 
t
e
s
t
e
d
.
 
M
e
a
s
u
r
i
n
g
 
a


l
o
n
g
 
a
d
v
i
c
e
 
t
h
e
r
e
f
o
r
e
 
r
e
q
u
i
r
e
s
 
f
i
r
s
t
 
w
i
d
e
n
i
n
g
 
`
s
t
a
l
e
_
a
f
t
e
r
_
m
s
`
 
a
b
o
v
e
 
t
h
e
 
a
d
v
i
c
e
,
 
o
r
 
t
h
e


`
h
b
_
a
d
v
i
c
e
_
m
s
`
 
k
e
y
,
 
w
h
i
c
h
 
s
e
p
a
r
a
t
e
s
 
t
h
e
 
t
w
o
 
n
u
m
b
e
r
s
.
 
T
h
a
t
 
i
s
 
a
 
f
a
c
t
 
o
f
 
t
h
e
 
c
o
d
e
,
 
n
o
t
 
a
n


a
s
s
u
m
p
t
i
o
n
 
a
b
o
u
t
 
t
h
e
 
c
a
m
e
r
a
:
 
t
o
d
a
y
 
t
h
e
 
a
d
v
i
c
e
 
c
a
n
n
o
t
 
e
x
c
e
e
d
 
t
h
e
 
w
i
n
d
o
w
,
 
s
o
 
a
 
l
o
n
g
-
a
d
v
i
c
e


r
e
a
d
i
n
g
 
i
s
 
p
o
s
t
p
o
n
e
d
 
u
n
t
i
l
 
o
n
e
 
o
f
 
t
h
o
s
e
 
t
w
o
 
c
h
a
n
g
e
s
 
i
s
 
i
n
 
p
l
a
c
e
.
 
U
n
t
i
l
 
t
h
e
n
,
 
n
o
t
h
i
n
g
 
i
n


t
h
i
s
 
s
e
c
t
i
o
n
 
s
h
o
u
l
d
 
b
e
 
r
e
a
d
 
a
s
 
a
 
r
e
s
u
l
t
.

### Never stop answering the beats

Answering a beat costs one UDP packet. Not answering costs the camera declaring its cloud
dead, which sends it back into discovery and re-registration: it keeps retrying, which on
a small network looks like a burst of refused connections, and it pays for the retries out
of the battery. Any change to the registration path keeps answering — including when the
payload is unexpected and when the camera is not in the configuration. Log those, answer
them, move on.

## What this bridge does not include

Pieces that run next to it and are deliberately not published here, so that the absence is
stated instead of implied: a private notebook recording what each camera announced
(superseded by the log line and the press event), a mirror of raw protocol frames, a chime
helper, and a stand-alone probe. None of them is needed by anything in this repository.
If one of them is needed, that is a request to make, not a gap to guess at.

## Cameras discovered at startup

Every camera this bridge manages is normally named in the file, because a uid is what a wake
request is addressed to, what the registry keys on, and what a cloud token is minted for.
`[startup_discovery]` is one table that lets the network answer that question instead:

| key | default | what it does |
| --- | --- | --- |
| `[startup_discovery].enable` | `false` | opt-in; off means no socket is opened and nothing is broadcast |
| `[startup_discovery].wait_secs` | `5` | how long to listen for answers, 1–60; startup waits no longer |
| `[startup_discovery].username` | — | the account adopted cameras are given |
| `[startup_discovery].password` | — | the password adopted cameras are given |

When it is on, the bridge broadcasts the protocol's any-camera request (`C2D_S`, UDP broadcast
to port 2015, repeated three times over the first second and a half and then left alone),
listens for `wait_secs`, and turns every answer that names a camera into a camera: address is
the address that answered, `discovery` is `local`, credentials are the pair above, and the name
is `discovered-` plus the uid's 16-character base form. Nothing else is invented — no port is
guessed, no other address is tried, no password is borrowed from a camera that *was* declared.

Three rules decide what an answer becomes:

- **a declaration wins.** A camera already in `[[cameras]]` is not added a second time, and keeps
  every field written for it. The match uses the same prefix rule the push listener and the
  wake server use, in both directions, because the 20-character form a heartbeat carries and the
  16-character form a binary header carries are one camera; and the longer form is the one kept
  when the camera is adopted, because that is the form a wake request has to be addressed with.
- **credentials are not guessable.** With no complete pair in the table, a camera that answers is
  named in a warning — by uid tail, with the address it answered from and the key that is missing
  — and is not adopted. A discovered camera is a camera nobody has written down, so there is no
  per-camera account to fall back to, and logging into one camera under another camera's identity
  is not a default worth having. `check-config` says the same thing before the bridge ever runs.
- **nothing is dropped without a line about it.** An answer that names no camera is counted and
  logged at `debug`, which is the difference between "the LAN is empty" and "something answered
  and this firmware does not identify itself" — a distinction worth having before anyone changes
  a camera.

What it costs, and what has not been measured. The broadcast is a wake-up call: a sleeping
battery camera that hears it spends battery answering it, which is why the mode is opt-in, why
the repeats stop after the burst, and why the window is bounded above. And no capture in this
repository shows a camera answering a request that does not name it — the packet is in the
protocol model and has never been sent by this code, while the request this bridge does send
(`C2D_C`) provably cannot help, since its reply `D2C_CR` carries no uid. So a window that hears
nothing is an expected outcome: it warns, in those words, and the bridge carries on with the
cameras that were declared. `docs/wake-local-addressing.md` records what the mode does and does
not change about the wake path, and the naming consequence the operator should know before
adopting anything: a discovered camera is named from its uid, and Home Assistant remembers entity
names longer than the file does — a camera adopted once and declared later under its own `name`
keeps the old entities and gains new ones.

## Options

Everything the engine reads, and where the rest of its tuning lives:

| option | default | what it does |
| --- | --- | --- |
| `[[cameras]].stream` | `"all"` | which RTSP streams this camera serves (see above) |
| `[[cameras]].mqtt.<…>` | per upstream | which entities of the camera are published at all |
| `[startup_discovery].<…>` | off | cameras found by asking the LAN, not by being written down (see above) |

The burst's numbers are **not** configuration. `GUARD_WINDOW`, `COMMAND_BUDGET`,
`TIME_BUDGET`, `SESSION_WAIT`, `SESSION_POLL`, the three refusal limits and the
three read ranks come from measurements on the owner's camera on 2026-11-13, and
each is named for the measurement it encodes at its definition. A knob that
invites a value nobody has measured is how a tuned path gets untuned; when there
is a second camera to measure against, they become options.

## Migration, French names → English

Log sentences are part of this migration too. The private tree that builds the deployed binary
still writes some of its journal lines in French; in the published tree they are English, and
translating the remaining ones is an item of the other branch, after this review. A journal line is
a diagnosis - it gets translated for the person reading it at three in the morning, and here that
person reads both languages.

Renamed on this branch, with the breaking ones called out:

| was | now |
| --- | --- |
| `bairelay reglages-audio` | `bairelay audio-settings` |
| `bairelay reglages-carillon` | `bairelay chime-settings` |
| `--interphone` | `--talk` (writes `<talkAndReplyVolume>`) |
| `--messages` | `--visitor-volume` (writes `<visitorVolume>`) |
| `--visiteur` | `--loudspeaker` (writes `<visitorLoudspeaker>`) |
| `--melodie` | `--melody` |
| `--silencieux` | `--silent` |
| `--sonner` | `--ring` |
| `{prefix}/{cam}/query/reglages[/{family}]` | `…/query/settings[/{family}]` |
| `{prefix}/{cam}/status/evenement` | `…/status/event` |
| `{prefix}/{cam}/status/salve` | `…/status/burst` |
| `config-familles-lues.json` | `config-families-read.json` |
| `[annonces] interroger` / `tenue_secs` / `intervalle_secs` | removed — see below |
| `[carnet] …` | removed — see below |
| `bairelay pont-audio` (a visible alias) | gone, not aliased: `bairelay audio-bridge` is the only spelling (see `docs/two-way-audio.md`) |
| `--entree FORMAT` (a visible alias) | gone, not aliased: `--input FORMAT` is the only spelling |
| french text or a guillemet in a payload | not a mistake: see “Values that stay french, and why” |

The flag names follow the tag each one writes, so `--loudspeaker` is the
visitor loudspeaker switch and `--visitor-volume` is the volume of the pre-set
message clips — the two are easy to confuse from the camera's XML, where both
tags start with `visitor`.

`--talk` writes `<talkAndReplyVolume>`, and on this installation it is never written. The
intercom volume is read and reported like any other leaf of `<audioCfg>`, then left alone: the
visitor loudspeaker stays mute by the operator's decision, and changing that would take a
request naming the switch. A capability that ships without that sentence reads as an
invitation, which is why the sentence is here and not in a commit message. The example further
up this page shows the syntax of the flag; it is not a suggestion to run it on this doorbell.

`Config` uses `deny_unknown_fields`, so an old `[annonces]` or `[carnet]` block
is a startup error naming the key. Delete the block: both tables configured
listeners whose modules are on the audio branch, and nothing on this branch read
them. Keeping them renamed would have been an option that looks documented and
governs nothing, which is the bug this branch just fixed in `stream`.

The state file is simply not read any more under its old name: the first start
treats every family as unproven, so entities come back read-only until the next
successful read. That is the safe direction of the mistake — a missing write,
never a wrong one.

### Tables that carried french names

| table | key | now |
| --- | --- | --- |
| `[annonces]` | `interroger` | `read_on_wake`, per camera |
| `[annonces]` | `tenue_secs` | `read_hold_secs`, per camera |
| `[annonces]` | `intervalle_secs` | `read_interval_secs`, per camera |
| `[annonces]` | the table itself | gone: announcements are decided per camera, not once for all |
| `[carnet]` | the table and its keys | gone, no successor: what the camera announces is published as the press event and logged, and a notebook file is not part of this bridge |

The keys moved from one table for everything to the camera they belong to, because two
cameras on one installation can need different rhythms: a doorbell and a camera watching a
road should not share an interval.

## Values that stay french, and why

Some french survives in this tree on purpose, and a reader who finds it should be able
to tell in ten seconds whether they are looking at a bug or at a tripwire. The rule
used on every pass of the english migration, and the list of what it kept:

1. **A tripwire.** Text whose job is to *match* french or a secret, so translating it
   would blind the check that protects the operator.
   - `REASON_INTERDITS` in `crates/mqtt/src/status.rs` contains `mot de passe`
     alongside `password`, `passwd`, `token`, `secret` and `apikey`. A reason that
     carries a credential is replaced by `reason hidden: it could have contained a
     secret` before it reaches `status/{family}/reason`; the french entry is there
     because the camera, the reference library and the operator's own logs are
     partly french. Removing the entry would keep the sentence pretty and let a
     password be retained on the broker.
   - the `FRENCH` word list in `src/published_text_guard.rs` and
     `crates/mqtt/src/published_text_guard.rs`, and the spec-table list in
     `crates/mqtt/src/discovery/settings.rs` (three places, because the
     discovery labels are the surface Home Assistant remembers longest). The
     guard asserts that everything
     the bridge publishes is ASCII and contains none of those words, so its own
     source has to name them. Homographs are excluded on purpose — `volume`,
     `budget`, `part`, `port`, `tour`, `camera`, `status` are french words too, and
     putting them in the list makes the guard fail on `Speaker volume`.
2. **A capture.** A measurement that quotes what the broker really carried, usually
   the value published by the version *before* the switch. Deleting it deletes the
   reason the guard exists.
   - `src/burst.rs`, the 13 November `status/burst` measurement: the camera answered
     `etat:"refus"` at four commands out of eight, both for `raison:"appui"` and for
     `raison:"mouvement"`. Those are payload bytes from the old build, not values the
     current build can emit (it publishes `state:"refused"` and `reason:"press"`).
   - the negative assertion in the same file, `!state.contains("\"state\":\"refus\"")`,
     which only has meaning while it spells the old french value.
3. **Non-ASCII punctuation that is published deliberately.** MQTT payloads are UTF-8
   and Home Assistant renders these without trouble; the only cost is that a
   punctuation control counts them, which is the lesser evil.
   - `«` and `»`. `publishable_reason_bounded` maps `<` to `«` and `>` to `»` so that
     **no raw XML can ever reach a retained topic**: a reason quoting the camera's own
     markup stays readable (`this camera's reply has no «volume» tag, so this setting
     cannot be written…`) without becoming a payload that a consumer might parse. The
     guarantee is asserted in `src/mqtt_dispatch.rs` by a test that refuses a reason
     containing `<`. Replacing the mapping with `[X]` would be ASCII at the price of
     changing a published value *and* rewriting the assertion that guards it, which is
     the class of risk this migration exists to retire.
   - ` […]`, appended by `cut()` in `crates/core/src/bc_protocol/settings.rs` when a
     rendered text is truncated. The truncation is by characters, never by bytes,
     because a camera answer carries UTF-8 and a byte-offset cut in the middle of an
     accent panics — a log line is meant to be read, not to be exhaustive.

Anything else that is french, or above ASCII, in a comment, a log message, a test name
or a published value is a migration bug: fix it where you find it, and if it is a
published value, give it a row in the table above.
