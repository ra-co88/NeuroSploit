# Design: Structured Event Protocol (Part A) + Durable State Layer (Part B)

Status: draft for maintainer review. Grounded in the tree at commit `92e8959` (main, v4.4.0).
All `file:line` references were verified against that tree in October 2026.

> Provenance note: the audit report file (`security-audit/AUDIT-REPORT-v4.4.0-2026-10-20.md`)
> was not available in this environment; the finding IDs and one-line recaps below come from
> the triage brief. Every cited code location was re-verified directly against the source;
> two finding summaries (L11, L12) were reconstructed from the code because their exact audit
> wording was not available — the reconstruction is marked where used.

---

# Part A — Structured Event Protocol

## 1. Problem & findings recap

The harness communicates progress by printing prose to stdout. Three independent
string-matching parsers consume it, plus two display renderers that classify the same
prose a fourth time:

- `RunLive::ingest` — `neurosploit-rs/app/src/repl.rs:52-104` (REPL background-run state).
- `ingestLine` — `web/server.js:1115-1195` (web console job state).
- `Ui::ingest` — `neurosploit-rs/app/src/tui.rs:60-95` (Mission Control TUI).
- `render_line` / `render_compact` — `neurosploit-rs/app/src/main.rs:2515-2543, 2548-2586`
  and `render_untagged` — `main.rs:2632-2656` (display classification; not state, but
  another copy of the same tag grammar).

Findings addressed:

- **L11** — `ingestLine` duplicates `RunLive::ingest` with drifted behavior (verified).
- **L12** — undocumented `tag: prose` protocol re-parsed in four places (verified).
- **M15** — one-shot web run that parks on quota/auth exhaustion hangs forever.
- **H13 (web half)** — web server's job/config persistence is non-atomic.

## 2. Goals & non-goals

Goals:

1. Versioned, machine-readable NDJSON event stream covering run start, phase transitions, agent lifecycle, findings, park/resume, budget warnings, checkpoint writes, run completion, and errors.
2. `repl.rs`, `server.js`, and `tui.rs` stop deriving state from prose. Drop `ingestLine` regexes.
3. Web server learns definite `parked` state; one-shot web runs that park terminate instead of hanging (M15 fix).
4. Zero change to interactive REPL UX and xterm stream: terminal keeps receiving byte-identical prose.
5. Backward compatibility: new web against old binary and vice versa still works (degraded).

Non-goals:

- Rewriting ~200 remaining prose sites in one change; they become `Log` events first.
- Changing SPA SSE contract; server maps events onto existing named events plus one new `parked` event.
- Replacing in-process mpsc channel with anything else.

## 3. Constraints

- Stream is `tokio::sync::mpsc::channel::<String>(256)` in `spawn_engagement` (`main.rs:1396`).
- Pool emits state changes as prose: park notice (`pool.rs:269-284`), operator-pause (`pool.rs:234`).
- Web spawns one-shot CLI jobs and REPL-backed jobs; both parse stdout+stderr lines through `ingestLine`.
- Windows supported (`install.ps1`, `setup.bat`) — fd-3 pipe not portable.
- Web console pins to binary version via `binaryVersion()` — usable for feature detection.
- Run id created in `spawn_engagement` (`main.rs:1322-1325`).
- `--offline` runs the full pipeline without model APIs — basis for executable tests.

## 4. Options considered

**O1. Inline sentinel on stdout** (e.g. `\u001e`-prefixed JSON line).

- Pros: single channel; works everywhere.
- Cons: still multiplexed with human output; sentinel corruption risk; stderr/stdout interleave hazard.

**O2. Dedicated fd (fd 3 / `--event-fd N`)**.

- Pros: clean separation; kernel backpressure.
- Cons: not portably creatable on Windows; fragile handle inheritance; would need Unix fast path + file fallback.

**O3. `--events <path>`: NDJSON file, appended by harness, tailed by consumers. (Recommended)**

- Pros: portable; crash-proof journal; consumers read at own pace; REPL keeps printing prose; in-process consumers skip file and receive typed events over mpsc.
- Cons: delivery latency depends on tailer; append-only file needs rotation (mitigated: lives in run/job directory).

**Chosen: O3.** Windows portability is a hard constraint. In-process mpsc upgraded from `String` to `Event`.

## 5. Recommended design

### 5.1 Schema — new module `crates/harness/src/events.rs`

Reuses `types::Finding` and `ModelRef::label()`. Phases and reasons are closed enums.

```rust
pub const EVENT_SCHEMA_VERSION: u32 = 1;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Envelope {
    pub v: u32,
    pub seq: u64,
    pub ts_ms: u64,
    pub run_id: String,
    #[serde(flatten)]
    pub event: Event,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Event {
    Log { line: String },
    RunStarted { run_id: String, target: String, mode: String, workdir: String },
    Phase { phase: Phase, display: String },
    AgentStarted { name: String, title: String, display: String },
    AgentFinished { name: String, status: AgentStatus, candidates: u32, model: String, display: String },
    Finding { finding: Finding, display: String },
    Parked { reason: ParkReason, message: String, recoverable: bool },
    Resumed { via: ResumeVia, display: String },
    Budget { used: u64, limit: u64, threshold: u32, message: String },
    Checkpoint { path: String },
    RunFinished { state: RunState, findings: u32, report_url: String, exit_hint: ExitHint },
    Error { agent: String, message: String },
    Tool { kind: ToolKind, text: String, agent: String },
}

#[derive(...)] pub enum Phase { Starting, Recon, Planning, Exploiting, Validating, Chaining, Complete }
#[derive(...)] pub enum ParkReason { Quota, Auth, Operator }
#[derive(...)] pub enum ResumeVia { Operator, FallbackModel, AutoRetry }
#[derive(...)] pub enum AgentStatus { Done, Failed, Skipped }
#[derive(...)] pub enum RunState { Complete, Stopped, Parked, Failed, Discarded }
#[derive(...)] pub enum ExitHint { Ok, Parked, Cancelled }
#[derive(...)] pub enum ToolKind { Exec, Danger, Net, Read, Edit, Tool }
```

Rules: Every variant with prose form carries `display`. Versioning: `v` frozen at 1 for 4.x; additive-only changes. Unknown variants skipped by consumers. One line per event; single-task writer per process.

### 5.2 Emission — `EventSink`

```rust
#[derive(Clone)]
pub struct EventSink {
    tx: tokio::sync::mpsc::Sender<Event>,
    file: Arc<std::sync::Mutex<Option<std::io::BufWriter<std::fs::File>>>>,
    run_id: Arc<OnceLock<String>>,
    seq: Arc<AtomicU64>,
}
impl EventSink {
    pub async fn emit(&self, e: Event);
    pub fn emit_sync(&self, e: Event);
}
```

`spawn_engagement` (`main.rs:1396`) builds one `EventSink`; channel becomes `mpsc::channel::<Event>(256)`. When `--events <path>` passed, file side opened and truncated at run start.

Pool's progress plumbing changes from `Sender<String>` to `EventSink`: `set_progress` becomes `set_events`. Subscription-CLI layer keeps producing tagged strings internally; pool converts at boundary.

`park_exhausted` emits `Event::Parked { reason: Quota|Auth, message, recoverable: true }`; `pause_gate` emits `Parked { reason: Operator }` / `Resumed`.

`--events <path>` is global clap flag (available on REPL), defaults to off.

### 5.3 State-critical emission sites (~30)

Pipeline: phase transitions, agent launch/finish, findings, errors, budget alerts.
Pool: park/resume state.
App: run start/finish, Ctrl-C handling.
Rest (~170 sites) become `Event::Log { line }`.

### 5.4 Consumers

**repl.rs** — `RunLive::ingest` becomes `RunLive::apply(&Event)`. String matching deleted. Parked sets `self.phase = "paused (…)"` and `ActiveRun.paused`.

Display: monitor prints `event.display` through existing `render_compact`/`render_line`. Prose strings snapshot-tested to equal today's. Xterm stream unchanged.

When REPL launched with `--events`, monitor serializes events into file and appends REPL-level events: `RunFinished` and `Checkpoint`.

**server.js** — `ingestLine` regexes deleted; per-job tailer replaces it. `startJob` passes `--events <JOBS_DIR>/<id>.events.ndjson` and starts `tailEvents(job, path)` (fs.watch on Linux/macOS, 250 ms poll fallback). Maps events onto existing SSE contract.

M15 fix (one-shot jobs): `run_mode` gains watch handle on pool's paused state. When parked flips true and stdin not TTY, emits `Event::RunFinished { state: Parked, exit_hint: Parked }`, flushes checkpoint, cancels pool, exits with code 75. `server.js` maps exit 75 to `phase='paused (quota)'`, `parked=true`, `resumable=true` with checkpoint. Set `NEUROSPLOIT_PARK_HOLD=1` to restore old hold-forever behavior.

Fallback: `server.js` computes protocol support from `binaryVersion()`: binary ≥ 4.5 → events mode; older or `NEUROSPLOIT_FORCE_STDIO_PARSE=1` → keep `ingestLine` on stdout.

**tui.rs** — mechanical: `Ui::ingest` consumes `Event`; prose matching deleted.

### 5.5 Terminal output

Nothing changes. `Event::display` strings are byte-identical replacements for today's prose (enforced by snapshot tests).

## 6. Migration / rollout

1. Release 4.5, step 1 (state layer, Part B) — no format changes.
2. Release 4.5, step 2 (events):
   - Add `events.rs` + `--events` flag; convert pool + ~30 sites to typed events; wrap rest as `Log`.
   - Switch `repl.rs` and `tui.rs` consumers to `apply(&Event)`.
   - `server.js`: version-gated events mode + tailer; `ingestLine` retained as fallback.
   - One-shot parked-exit (exit 75) lands here.
3. Release 5.0 — delete `ingestLine`, delete `finding_json:` prose, delete parked-exit env override.

Compat matrix: Old web + new binary → no `--events` flag passed, prose works, `ingestLine` still works. New web + old binary → version gate → `ingestLine` fallback.

## 7. Test plan

- Unit (Rust, events.rs): serde round-trip, unknown variant skipped, seq strictly increasing, display snapshot tests.
- Unit (Rust, pool): `park_exhausted` emits `Parked{Quota|Auth}` exactly once; `pause_gate` emits `Operator` park/resume.
- Integration (app): `neurosploit run --offline https://target.test --events /tmp/e.ndjson` → parse NDJSON: first `run_started`, last `run_finished`, seq strictly increasing, phase sequence correct.
- Integration (parked): one-shot path exits 75, emits `RunFinished{state: Parked}`, writes checkpoint.
- REPL quit stress: spawn binary piped, `/run` (offline) + `/quit` 50×; assert `runs.json`, `session.json`, `checkpoints/*.json` all parse.
- Web: Node fixture test: feed recorded `events.ndjson` to `ingestEvent`, assert job snapshot shape and SSE event sequence; truncated final line; unknown variant.
- E2E (manual): launch REPL-backed job, `kill -9` server mid-run, restart, Resume — engagement carries findings forward.

## 8. Open questions

1. **Parked one-shot semantics (M15)**: exit code 75 + checkpoint + resumable job — approved? Alternative: keep parking, have `run_mode` read `/continue` from piped stdin.
2. **Version gate**: `binaryVersion()` gating acceptable, or prefer capability probing?
3. **Schema promise**: freeze `v: 1` additive-only through 4.x; delete prose parsing in 5.0?
4. **`AgentSelected` roster**: include full pinned/selected agent list in planning `Phase` event (larger line, enables per-agent panel early) or infer from `AgentStarted` events only?
5. **tui.rs timing**: convert in same change as `repl.rs` (recommended) or defer?

---

# Part B — Durable State Layer

## 1. Problem & findings recap

All durable state written with bare, non-atomic `fs::write`; every reader silently falls back to empty on failure:

- **H13** — `runs.json` (repl.rs:2321-2324), `active_run.json` (repl.rs:2328-2330), `session.json` (repl.rs:2380). Readers all `.ok()`/`.unwrap_or_default()`. Torn/corrupt file reads as empty; next save overwrites history. Web twin: `writeConfig`, `persistJob`, `saveEngagementName` (server.js).
- **M14** — `/quit` sets `cancel`, saves session, breaks immediately. Monitor task may concurrently write `runs.json` or checkpoint. Monitor handle not retained, REPL cannot wait for it.
- **M16** — checkpoint path global: `.neurospilot/active_run.json`. Two concurrent sessions clobber each other; completed run in A clears B's checkpoint.
- **M17** — `std::env::set_var` races concurrent readers: `set_key` writes provider env var; `spawn_engagement` writes `NEUROSPLOIT_POCS`, `NEUROSPLOIT_PROXY`, `NEUROSPLOIT_UA`; readers call `env::var` from other threads.
- **L8** — checkpoint written only on finding arrival. Commands, phase, pause state between findings lost on quit/crash.
- **L9** — run-completion flag `AtomicBool` with `Ordering::Relaxed` both sides. Store intends to publish history-write + `clear_checkpoint`; `Relaxed` load can observe done without happens-before, so queue loop may start next target before previous cleanup visible.

## 2. Goals & non-goals

Goals:

1. No torn or lost state file for any interleaving of crash, `kill -9`, `/quit`, or concurrent sessions.
2. Corrupt state file quarantined, recovered-from, never silently defaulted.
3. Checkpoints per-run and written on policy making L8 impossible: any meaningful state change plus periodic tick.
4. `/quit` waits for background monitor instead of racing it (M14).
5. All cross-thread state flags use ordering that publishes what they mean (L9).
6. No `env::set_var` writes at runtime (M17).
7. Web server's own files get same guarantees (H13 web half).

Non-goals: Introduce database. Change on-disk format of `runs.json` / `session.json`. Encrypt/access-control state files.

## 3. Constraints

- State in `<cwd>/.neurospilot/` (proj_dir); run artifacts in `<base>/runs/<run_id>/`.
- Recovery REPL-startup-only: `load_checkpoint` → fold into history → `auto_resume` when piped.
- Pool exposes handles for pause/resume/fallback/cancel; REPL holds them in `ActiveRun`.
- Project deliberately slim: 6 workspace deps; web server zero-dependency. Docs say "No DB needed".
- Windows supported — locking and atomic-rename must be portable.
- `write_status` per run read by web UI.

## 4. Options considered

**O1. Atomic-file layer (tmp + fsync + rename, per-run namespacing, locks, quarantine) — JSON stays. (Recommended)**

- Pros: no deps; same files/schemas/tools; ~30-line primitive; failure modes few/testable; rename-atomicity guaranteed POSIX + Windows.
- Cons: readers handle quarantine/recovery; crash between fsync/rename leaves `.tmp` orphan (harmless, cleaned on next start); no queries/indexes (irrelevant at scale).

**O2. SQLite via `rusqlite` (bundled)**.

- Pros: real transactions, journaling, concurrent readers, queries.
- Cons: one more native crate (slow CI, larger binary); web needs JS reader (zero-dep violation) or new API; files not human-inspectable; project explicitly rejects DB.

**O3. Hybrid (atomic files now, SQLite trait later)**.

- Cons: speculative generality; seam exercised by one implementation indefinitely.

**Chosen: O1.** Data is three small JSON documents; every O2 benefit either unneeded or achievable with locks. O3 rejected as speculative.

## 5. Recommended design

### 5.1 New module `crates/harness/src/state.rs`

```rust
pub fn write_atomic(path: &Path, data: &[u8]) -> io::Result<()>;
pub enum Load<T> { Fresh, Value(T), Corrupt { quarantined: PathBuf } }
pub fn load_json<T: DeserializeOwned>(path: &Path) -> Load<T>;
pub struct StateLock { /* .. */ }
pub fn lock(name: &str) -> io::Result<StateLock>;
```

Locking uses O_EXCL lock files (portable, no deps); contention ≤2 processes. Lock files record pid+timestamp; stale lock (dead pid or >60s) stolen with warning. Scope strictly read-modify-write of one file, never across `.await`.

`.bak` rotation: `write_atomic` renames current to `<path>.bak` before renaming temp in. Two cheap renames per save. Gives H13 two-deep recovery: corrupt main → `.bak`; corrupt both → quarantine + fresh start with warning.

### 5.2 Module API

All in `repl.rs` today; functions keep names, call primitives, callers barely change:

```rust
pub(crate) fn with_history<T>(base: &Path, f: impl FnOnce(&mut Vec<RunRecord>) -> T) -> Option<T>;
fn load_runs(base: &Path) -> Vec<RunRecord>;
fn save_runs(base: &Path, history: &[RunRecord]);
fn save_session(s: &Session);
fn load_session(s: &mut Session) -> bool;
fn checkpoint_path(run_id: &str) -> PathBuf;
fn save_checkpoint(c: &LiveCheckpoint);
fn clear_checkpoint(run_id: &str);
fn load_checkpoints() -> Vec<LiveCheckpoint>;
```

`LiveCheckpoint` gains `run_id: String` and `updated: u64` with `#[serde(default)]` — old files still parse.

Namespacing (M16): run id exists at spawn, monitor checkpoints into `.neurospilot/checkpoints/<run_id>.json`; `clear_checkpoint` takes run id and can't erase another session's file.

Legacy migration: at REPL startup, if `.neurospilot/active_run.json` exists, treat as checkpoint (recover → clear → delete). For 4.5, state layer dual-writes `checkpoints/<run_id>.json` and legacy `active_run.json` so downgrade mid-project never loses checkpoint; dual-write removed in 5.0.

### 5.3 Checkpoint policy — events-driven (L8), wired to Part A stream

Monitor task owns small `StateTracker`:

- On `Event::Finding` → mark dirty.
- On `Event::Phase`, `Event::AgentStarted`, `Event::AgentFinished`, `Event::Tool` → mark dirty.
- On `Event::Parked` → flush immediately.
- On existing 15 s ticker (repl.rs:1995) → flush if dirty; clear dirty.
- On `Event::RunFinished` and Discard path → clear.

Same tracker runs inside `run_mode`, giving one-shot CLI and web jobs crash recovery (and making web's parked-exit resumable).

### 5.4 Cross-thread flags and shutdown (M14, L9)

L9: every `done` store becomes `Ordering::Release`; every load becomes `Acquire`. Publication chain history-write → `clear_checkpoint` → `done.store` then visible to queue loop and `/quit`. Pool's `paused` CAS already correct (`AcqRel`/`Acquire`); new watch mirror from Part A updated at same three sites, eventual by design.

M14: `start_background` retains monitor's `JoinHandle` in `ActiveRun`. `/quit` and Ctrl-C-quit: set `cancel` → `tokio::time::timeout(10s, handle)` → `save_session` → break. Monitor ignoring deadline abandoned; atomic-write layer + event-driven checkpoint bound loss to one tick instead of torn file.

### 5.5 Environment writes (M17)

`set_key`: stop writing `std::env::set_var`. Provider keys set mid-session go into `Session` (keys: BTreeMap) and passed to run via new `RunConfig::api_keys: BTreeMap`. Key resolution checks map first, then environment. Env keys at launch kept; `/key` printout reads merged view.

`spawn_engagement`: `NEUROSPLOIT_POCS`, proxy, UA move to `RunConfig` fields. Three harness readers take values from `RunConfig`/explicit args; `env::var` read-only fallback. `RunConfig` already flows to every reader (plumbing only).

### 5.6 Web-server mirror (H13 web half)

Same primitives in JS inside `server.js` (zero-dep preserved):

```js
function atomicWriteSync(p, data) { /* tmp + fsync + rename; .bak rotation */ }
function readJson(p, fallback) { /* ENOENT→fallback; parse error → quarantine + .bak */ }
```

Applied to `writeConfig`/`readConfig`, `persistJob`, `saveEngagementName`. `persistJob` moves to `atomicWriteSync` with throttle unchanged.

## 6. Migration / rollout

1. Order: state layer first (independent, zero format change), events second, tracker wiring last.
2. On-disk compat:
   - `runs.json`, `session.json`: byte-identical schemas; readers tolerate both. No migration.
   - `active_run.json` → `checkpoints/<run_id>.json`: startup reads both; dual-write one release (4.5); legacy deleted after recovery. Downgrade risk after 5.0 documented.
   - New files (`locks/`, `.tmp-*`, `.corrupt-*`, `.bak`) additive; `proj_dir()` creates subdirectories lazily.
3. Web server: JS primitives drop-in; job files keep schema; SPA untouched.
4. Behavior changes: `/quit` may take ~10 s for monitor finalization (previously instant with corruption risk); corrupt state file announces itself on stderr, preserved as `*.corrupt-*` instead of vanishing.
5. Compat with event stream: checkpoints visible as `Event::Checkpoint`; nothing else couples.

## 7. Test plan

- Unit (state.rs): `write_atomic` leaves no tmp orphans, content+fsync observable, `.bak` holds previous. Crash simulation: `load_json` returns A not error. Corrupt main → recover from `.bak`; corrupt both → `Fresh` + quarantine. Contention across processes; stale-lock steal. Per-run checkpoints never collide; `clear_checkpoint(a)` leaves `b` intact.
- Unit (ordering, L9): monitor completion sequence under stress with reader thread until `done == true` observed, then assert `runs.json` contains run (catches `Relaxed` regression).
- Integration: piped REPL `/run` (offline) + `/quit` 50×; assert `session.json`, `runs.json`, `checkpoints/*.json` parse, no `.tmp-*` files remain.
- Web: Node fixture tests for `atomicWriteSync` (mock throw in write → previous intact) and `readJson` quarantine.
- E2E: kill -9/restart/Resume scenario — exercises checkpoint naming, recovery, relaunch.

## 8. Open questions

1. **Locking**: O_EXCL lock files (portable, no deps, stale-steal logic) vs `flock` Unix + `LockFileEx` Windows via small `unsafe` shim?
2. **Dual-write window**: keep legacy `active_run.json` dual-write one release (yes) or drop immediately?
3. **`/quit` timeout**: 10 s ceiling for monitor wait (yes)?
4. **`RunConfig::api_keys`**: keys visible in debug print — confirm `RunConfig` never logged wholesale (none found; recommend `#[serde(skip_serializing)]` + manual `Debug` masking).
5. **status.json**: move `write_status` onto `write_atomic` (yes — read by web UI)?

## Decisions summary

| Decision | Chosen | Runner-up | Why |
|---|---|---|---|
| Event transport | `--events <path>` NDJSON + typed mpsc in-process | inline sentinel, fd 3 | portable; crash-proof journal; no prose multiplexing |
| Emission scope | ~30 typed sites, rest `Log` | big-bang conversion | reviewable diff; parsers unhooked immediately |
| State backend | atomic-file layer, JSON stays | SQLite | zero deps; human-inspectable; DB buys nothing at scale |
| Checkpoint naming | `.neurospilot/checkpoints/<run_id>.json` | global file | concurrent sessions can't clobber (M16) |
| One-shot parked (M15) | checkpoint + exit 75 + resumable | hold forever / piped stdin | run nobody can resume must not hold process |
| `done` ordering (L9) | `Release`/`Acquire` on bool | `watch` channel | minimal, auditable; channel fallback if flags grow payloads |

## Assumptions

- L11, L12 audit wording unavailable; treated as verified web-parser drift + undocumented line grammar. Both verified to exist independently.
- Maintainer wants zero-dependency web server and slim Rust deps (stated in code comments).
- `--offline` runs acceptable as CI workhorse.

## Out of scope

- Making `host/aitest/skills` web jobs REPL-backed (gain checkpoints from §5.3; full resume later).
- Query/index run history (SQLite threshold: thousands per project).
- Encrypting state files or multi-project locking.
- Converting remaining ~170 prose sites to typed events (incremental, post-4.5).
