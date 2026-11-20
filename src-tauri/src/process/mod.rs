//! Windows process-supervision primitives.
//!
//! Provides RAII Win32 Job Object containment and process supervision used
//! by DownloadManager media extraction workers.

#[cfg(windows)]
#[allow(dead_code)] // Foundation used by the isolated process supervisor.
mod windows_job;

#[cfg(windows)]
#[allow(dead_code)] // Spawn adapter remains isolated from production execution.
mod windows_spawn;

#[cfg(windows)]
#[allow(dead_code)]
mod supervisor;

#[cfg(windows)]
pub(crate) use supervisor::{
    MonitoredOutput, OutputHandler, ProcessOutcome, ProcessStream, run as run_supervised,
    run_with_output as run_supervised_with_output,
};

#[cfg(windows)]
pub(crate) use windows_spawn::ProcessSpec;

#[cfg(all(test, windows))]
mod tests;

#[cfg(all(test, windows))]
pub(crate) use tests::{assert_process_terminated, process_fixture_spec_for_integration};
