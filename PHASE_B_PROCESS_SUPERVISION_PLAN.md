# FluxNet Download Manager — Phase B Process Supervision Plan

**Status:** feasibility/design only  
**Date:** 2026-11-16  
**Scope:** Windows child-process supervision groundwork before any yt-dlp or FFmpeg integration. No production code or behavior changed. No dependencies were downloaded/installed and no child-process fixture was run.

## Summary

FluxNet already has a single Rust `DownloadManager` that owns queue state, concurrency, history, pause/resume/retry/cancel and completion. It currently downloads with Rust async HTTP and has no media-tool child-process supervisor. `std::process::Command` is used only to open Explorer after completion; there is no reusable download subprocess abstraction.

Implement a Windows-specific, RAII-based process runner as a library used *inside one existing manager worker*. The manager remains responsible for admission and task status; the process runner owns only the lifetime, pipes and resource envelope of that worker's child process tree. Do not add a daemon, RPC service, child-managed task queue or separate progress/history model.

Use the already-present `windows-sys` crate and extend its enabled API features for Windows Job Objects and process/thread creation before considering a new crate. Tokio, futures, `std::process` for the existing Explorer call, atomics, `Notify`, and app-owned task controls are already present. Tokio's `process` feature is not currently enabled; however, using `tokio::process::Command` alone does not provide race-free suspended creation and Job Object assignment. A small native `CreateProcessW(CREATE_SUSPENDED)` layer is the safer fit when strict descendant containment is required.

## 1. Current findings

### Queue/task ownership

- `src-tauri/src/download_manager.rs` defines `DownloadManager`, its persisted entry collection, `controls`, `ConcurrencyLimiter`, `BandwidthLimiter`, and `worker_count`.
- `spawn_worker` is called by `start_download`, resume and retry. `download_worker` obtains the existing manager concurrency slot and performs the work.
- `DownloadTaskControl` currently uses `AtomicBool` flags for cancellation and pause plus a Tokio `Notify`. Those signals are consumed by HTTP/HLS workers. A subprocess adapter should receive this same task control (or a narrow borrowed cancellation interface), not create a new queue or user-visible task.
- The manager's terminal statuses are `Completed`, `Failed`, and `Cancelled`; current progress events/history are manager-owned. Process exit must feed a manager result, never independently mark a job complete.
- On shutdown, `cancel_all_downloads` requests cancellation and waits for manager workers. An active process runner must observe cancellation, close/terminate its job, reap/wait its process, settle output handles, and then return so that manager shutdown can finish.

### Existing timeouts, dependencies and tests

- HTTP uses `reqwest` connect/read/request timeouts. They do not apply to a future child process.
- Cargo dependencies already include Tokio with `fs`, `io-util`, `macros`, `sync`, and `time`; `futures-util`; and Windows-targeted `windows-sys = 0.61`.
- `windows-sys` currently enables Foundation, FileSystem, IO, Pipes and other APIs, but its manifest does **not** currently enable `Win32_System_JobObjects` or `Win32_System_Threading` (or all features needed for process security attributes). Those are API feature additions to the existing dependency, not a new crate.
- Tokio's `process` feature is absent and `tokio-util` is not a direct dependency. Do not add `tokio-util` just to obtain cancellation tokens until the existing manager signal has been evaluated.
- There are extensive unit tests in `download_manager.rs`, including concurrency-release tests and temp-file helpers. There are no process supervision or Windows Job Object tests. This plan does not run tests.
- `open_download_containing_folder` launches `explorer.exe`; it is outside download worker lifecycle and should not be migrated into the media worker abstraction as part of Phase B.

## 2. Proposed module design

```text
src-tauri/src/process/
  mod.rs                  # ProcessSpec, ProcessLimits, ProcessOutcome, safe error contract
  supervisor.rs           # async orchestration, timeout/cancel/output drain coordination
  windows_job.rs          # RAII Job Object and kill-on-close/resource limit setup
  windows_spawn.rs        # suspended CreateProcessW, explicit handle inheritance, job assign/resume
  output.rs               # bounded stdout/stderr drains, progress parser input and redaction boundary
  task_workspace.rs       # unique task temp root, containment and cleanup policy
  tests.rs                # harmless local fixture tests (Windows-gated where necessary)
```

The Rust Download Manager calls this module from its existing worker after queue-slot acquisition. The module accepts one fixed executable, argument vector, environment policy, working directory, limits and the manager's cancellation/pause signals; it returns one typed result and normalized output/progress events. It cannot create queue entries or write download history.

### Process specification

```rust
struct ProcessSpec {
    executable: AbsoluteVerifiedPath,
    args: Vec<OsString>,
    current_dir: TaskWorkspace,
    environment: ExplicitEnvironment,
    stdin: StdinPolicy,              // normally closed/null
    limits: ProcessLimits,
    output_policy: BoundedCapturePolicy,
}

struct ProcessLimits {
    wall_timeout: Duration,
    idle_timeout: Option<Duration>,
    stdout_capture_bytes: usize,
    stderr_capture_bytes: usize,
    max_active_processes: u32,
    process_memory_bytes: Option<usize>,
    cpu_time: Option<Duration>,
}
```

Validate the executable as an absolute path under an approved application resource/tool directory; reject a working directory outside the unique task workspace. Arguments remain an `OsString` vector. Never construct a shell command string, invoke `cmd.exe`/PowerShell for media tools, or interpolate URLs/filenames into a shell. Restrict environment variables and redact URL query/auth values from diagnostics.

### Job Object setup and race-free descendant containment

1. Create a per-child Job Object and immediately configure `JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE`. Use an RAII handle wrapper that closes on every Rust return/unwind path.
2. Apply reviewed resource controls where supported: active process count; per-process or job memory; optional CPU-time/rate limit. Do not select arbitrary limits until yt-dlp/FFmpeg fixture measurements exist. A Job Object does not impose a scratch-disk quota, so enforce output bytes in the worker and workspace volume policy separately.
3. Create only the required inheritable stdout/stderr/stdin pipe handles. Mark all unrelated handles non-inheritable and use an explicit handle list where the API/OS version permits.
4. Call `CreateProcessW` with a correctly quoted mutable Windows command-line buffer, explicit application name, explicit working directory/environment, `CREATE_SUSPENDED`, and only intended standard handles.
5. Assign the suspended process to the Job Object before it executes, then resume its primary thread. This closes the important race in which a child could spawn descendants before assignment. If job creation/configuration/assignment fails, terminate the suspended process and fail closed; do not launch it outside the job as fallback.
6. Keep the Job Object handle alive for the worker lifetime. On cancellation, timeout, manager shutdown, supervisor error or drop, terminate the Job Object (or close it under kill-on-close), wait for the root process to exit, drain/close pipes, and only then release task resources.

Nested-job constraints and Windows version behavior need testing. If FluxNet itself is hosted under an enclosing job which disallows assignment, fail closed with a specific `JobAssignmentUnavailable` error rather than silently weakening descendant cleanup.

### Wait, cancellation and timeout

- Use one supervisor state machine driven by the manager's cancellation signal, optional pause signal, wall deadline, idle deadline, process completion and pipe-reader completion.
- A native process wait can block. Do not call an unbounded blocking wait on a Tokio runtime worker. Use a dedicated blocking waiter/registered wait and send completion back to the async supervisor; enforce deadlines in the async layer.
- Cancellation and timeout are manager-visible outcomes, not successful child exits. Terminate the whole job, wait/reap, and return `Cancelled` or `TimedOut`; suppress late progress after terminal state.
- For Phase B, define pause as **stop-and-resume only when the tool's partial-file contract is validated**. Do not fake a paused status while the child continues. If pause has no safe protocol, reject/disable pause for that engine initially and preserve existing Rust HTTP pause behavior.
- Add a short bounded termination grace only if the entire job remains contained during it; after grace, force termination. Cancellation must not wait indefinitely for a child that ignores graceful shutdown.
- Ensure races are deterministic: if cancellation wins before commit, the manager must not transition the job to `Completed`. Process success is necessary, not sufficient, for completion.

### Bounded stdout and stderr

- Drain both pipes concurrently from process start; waiting without draining can deadlock when a child fills a pipe.
- Retain only a fixed-size prefix/tail per stream and set a `truncated` flag; continue draining/discarding beyond the cap so the child cannot block. Bound any line/progress parser buffers too.
- Decode lossily for diagnostic text, normalize control characters, redact secrets/URLs/paths, and return structured fields (`exit code`, `timed out`, `cancelled`, `stdout truncated`, `stderr truncated`, allowlisted progress). Never expose raw unbounded output to the UI or logs.
- If progress is parsed, parse only a documented machine-readable channel/template. Keep human-readable stderr as bounded diagnostics and do not rely on fragile localized status text.

### Per-task working directory and temporary-file cleanup

- Use a unique directory under FluxNet's app-owned cache/temp root, e.g. `.../process-tasks/<task-id>-<random>/`; never use the executable directory, repository checkout, or current working directory.
- Create it with restrictive ACLs inherited from the app-data location; reject reparse points/symlinks and canonicalize containment before writes/deletes. Set child cwd explicitly. Put tool config/cache/temp under this root through explicit environment/options.
- A workspace RAII guard cleans only files it created after all process handles close. Do not recursively delete caller-selected destination folders, pre-existing partial downloads, or paths reconstructed from untrusted tool output.
- Distinguish scratch from resumable output. Scratch is removed on success/failure/cancel according to policy; a partial download is retained only when the manager has validated its identity/offset and records the task state. A failed cleanup is recorded as a safe cleanup warning, not disguised as success.
- Set byte caps for child-produced files and monitor free space before/during processing. Job Objects do not cap disk. Prevent path traversal in tool output names by using fixed output templates and mapping only approved task-relative files.

### Resource limits and typed errors

Start with conservative measured limits instead of unsupported guesses:

- Per-task wall timeout and optional no-output/no-progress idle timeout.
- Bounded logs/progress, maximum direct descendants, and configurable process memory/CPU limits where practical.
- Maximum concurrent external-process jobs is no greater than the existing DownloadManager concurrency setting; use the same slot, not a second semaphore/queue.
- Fixed per-task scratch/output byte budget and minimum free-space reserve checked by the manager/staging layer.
- Typed internal errors: `SpawnDenied`, `JobObjectUnavailable`, `JobAssignmentFailed`, `TimedOut`, `Cancelled`, `NonZeroExit`, `PipeReadFailed`, `OutputLimitExceeded`, `ResourceLimitExceeded`, `WorkspaceViolation`, `CleanupFailed`, and `SupervisorPanicked`.
- Map internal errors to short safe user messages; raw OS errors and tool output remain bounded/scrubbed diagnostics. Do not include command-line URLs, access tokens, or full local paths in persisted history/events.

## 3. Required dependencies and API additions

### Reuse first

| Existing dependency/API | Reuse |
|---|---|
| `windows-sys 0.61` (Windows-only direct dependency) | Add only required Cargo feature flags for Job Objects, process/thread APIs and security attributes/pipes. Implement a small audited RAII wrapper over Win32 handles. Avoid adding a second Windows bindings crate unless safety/review needs justify it. |
| Tokio `time`, `sync`, `io-util` | Deadlines, manager signal coordination, bounded stream reading. These features are already enabled. |
| `futures-util` | Existing async utilities; do not add a process runner crate just for stream orchestration. |
| Existing `DownloadTaskControl`, manager worker and concurrency slot | Reuse manager cancellation/pause lifecycle and concurrency ownership; expose narrow references/interfaces to the process layer. |
| Rust `OsString`, `PathBuf`, `std::io`, RAII/Drop | Argument transport, path handling, handle/resource guards. |

### Likely Cargo feature-only changes in a future implementation

- `windows-sys`: `Win32_System_JobObjects`, `Win32_System_Threading`, and any precisely needed `Win32_Security`/pipe/handle features. Verify exact API feature gates against the pinned windows-sys 0.61 docs at implementation time.
- Tokio `process` is optional only if a design uses Tokio child pipes; it does not solve the suspended-create/assign/resume race by itself. A direct Win32 process wrapper can avoid enabling it.
- No new crate is currently justified. Consider `tokio-util` only if a later design requires its cancellation primitives and demonstrates that the existing manager `AtomicBool` + `Notify` control cannot satisfy requirements. Do not add `duct`, `portable-pty`, or another subprocess crate without a concrete gap and license/size review.

## 4. Harmless local child-process fixture strategy

Use a purpose-built test-only fixture executable, not yt-dlp, FFmpeg, network URLs, arbitrary shell commands or production resources. It should implement a small fixed set of behaviors selected by enum-like arguments:

- Exit with a chosen status immediately.
- Sleep beyond timeout or wait until terminated.
- Emit known stdout/stderr text and progress lines.
- Flood both streams beyond configured caps.
- Spawn one harmless descendant (the same fixture binary) that waits, then exit its parent; record only test PIDs under a temporary test directory.
- Create a small task-relative file and an attempted `..` path for containment tests.
- Accept arguments containing spaces, quotes, Unicode, ampersands, percent signs and shell metacharacters, then echo a safe argument digest to prove literal argv transport.

The fixture must never access the network, inspect user data, change system state, or execute arbitrary supplied commands. Put its build output in the test target directory and its runtime files in a unique OS temp test directory. Gate Job Object tests to Windows; keep argv/redaction/limits/error mapping unit tests portable where possible.

## 5. Test cases and acceptance criteria

| Test | Required result |
|---|---|
| Literal argv with spaces, Unicode, quotes and shell metacharacters | Child receives the intended argument boundaries; no shell/interpreter is involved. |
| Valid absolute executable and explicit cwd | Child sees only task cwd and fixed environment; invalid relative/outside executable path is rejected. |
| Missing executable / access denied / invalid cwd | Typed spawn error; no orphan process or leaked workspace/handle. |
| Immediate successful and nonzero exit | Correct exit classification; manager alone decides task status. |
| Stdout/stderr flood beyond cap | Both pipes continue draining; retained text never exceeds configured cap; truncation flag set; no deadlock or large memory growth. |
| Wall timeout and idle timeout | Entire job is terminated/reaped within bounded deadline and reported as timeout. |
| User cancellation and app shutdown | Whole process tree is terminated, root is waited, pipes close, worker returns and manager shutdown completes. |
| Parent exits while descendant remains | Job Object kill-on-close/termination removes descendant; no child survives worker completion. |
| Process spawns at startup | Suspended creation and job assignment occur before child code runs; fixture cannot escape the job during startup. |
| Job creation/assignment/resume failure injection | Fail closed; suspended process is terminated; no uncontained process runs. |
| Active-process/memory/CPU limit | Configured limit is enforced and mapped to a resource-limit error; no UI completion. |
| Per-task path containment and cleanup | Child-created task files are isolated; traversal/reparse-point attempt is rejected; only owned scratch is removed. |
| Cleanup failure and disk-full simulation | Safe warning/failure recorded; never report successful completion without later validation/commit. |
| Cancel-versus-exit race | A cancellation accepted before commit wins; no late event changes terminal Cancelled to Completed. |
| Manager concurrency integration | Process worker consumes one existing queue slot; setting max concurrency remains authoritative; no extra queue/semaphore is introduced. |
| Regression | Existing HTTP Range/resume, HLS, pause/cancel/retry/history and direct-download behavior remains unchanged. |

Acceptance gates: on Windows, every timeout/cancel/shutdown test leaves zero fixture descendants; output capture remains within configured bounds; all handles/workspaces are released; the process layer creates no DownloadEntry; and existing Download Manager regression tests pass without behavioral changes. No external website or media-tool binary is needed for these Phase B tests.

## 6. Risks and design cautions

1. **Spawn/assignment race:** `std::process::Command` followed by `AssignProcessToJobObject` allows a short interval in which the child can spawn descendants. A suspended native creation path is required for the stated strong guarantee.
2. **Win32 unsafe code:** raw handles, pipe inheritance, command-line quoting, environment blocks, wait semantics and error cleanup require narrow audited `unsafe` wrappers and extensive failure-injection testing.
3. **Nested Job Objects:** another supervisor/CI/host job may prevent assignment or impose limits. Fail closed and report environment constraints; do not silently fall back to root-only termination.
4. **Pipe deadlocks and memory exhaustion:** both pipes must drain concurrently; every capture/parser buffer must be bounded while readers continue draining discarded output.
5. **Pause semantics:** arbitrary subprocesses cannot be safely paused just by setting the manager's current pause flag. Keep current HTTP pause unchanged and define engine-specific process pause only after a validated checkpoint contract.
6. **Cancellation races:** process exit, cancel request, final validation and atomic rename can race. The manager must serialize state transition/commit so cancellation cannot be overwritten by a late successful process event.
7. **Resource policy uncertainty:** memory/CPU/process limits can break legitimate high-resolution processing. Establish defaults from harmless fixture and later representative-media measurements; limits should be explicit and observable.
8. **Workspace trust:** lexical path checks do not defeat reparse-point redirection. Create private task roots and verify canonical paths/handles before cleanup.
9. **Job Object is not a sandbox:** it limits process lifetime/resources; it does not restrict filesystem or network access, prevent a child from reading user files, or validate output. Use least privilege, fixed executable paths, controlled environment and later network policy separately.
10. **No new downloader behavior yet:** this layer does not imply yt-dlp/FFmpeg integration, media validation, or website support. Those remain later approved phases.

## 7. Implementation order

1. **Lock current behavior:** document manager lifecycle invariants and add process-layer interfaces without redirecting any existing download path.
2. **Add minimal Windows API features:** extend the existing `windows-sys` feature set only; confirm API signatures and supported Windows versions. No new third-party crate initially.
3. **Implement handle and Job Object RAII:** create/configure/close job, kill-on-close, limits, and failure cleanup in isolation.
4. **Implement suspended spawn:** explicit executable/argv/cwd/environment/stdio handles, assign-before-resume, fail-closed cleanup.
5. **Implement bounded pipe readers and waiter:** concurrent draining, bounded capture, timeout/idle deadline, cancel/shutdown integration, wait/reap.
6. **Implement workspace guard and typed errors:** private per-task paths, containment, bounded output files, ownership-aware cleanup.
7. **Add local fixtures and Windows tests:** test all failure paths and descendant cleanup before any real tool executable is allowed.
8. **Integrate as an unused adapter seam in DownloadManager:** one existing worker and concurrency slot own one process call; do not change the current direct HTTP/HLS dispatch.
9. **Review security and compatibility:** code review the unsafe Win32 layer, verify current Windows versions/nested-job behavior and test app shutdown. Only after approval consider yt-dlp or FFmpeg in a later phase.

## Final recommendation

The process-supervision layer is feasible without a new crate and without replacing FluxNet's Download Manager. Reuse the existing `windows-sys`, Tokio timing/synchronization/I/O, and manager control lifecycle. For a strong descendant-cleanup guarantee, use a narrowly scoped Windows native suspended-create → Job Object assign → resume sequence; Tokio process management alone is not a substitute for this ordering. Keep the module inert with respect to existing downloads until the Windows fixture suite demonstrates bounded output, fail-closed assignment, cancellation, timeout, descendant cleanup and workspace containment.

No production implementation should begin until the Win32 API design, nested Job Object behavior, pause semantics and resource defaults have been reviewed. This document is the only requested deliverable for this phase.
