use std::{
    io,
    mem::size_of,
    os::windows::io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
    ptr::{null, null_mut},
    thread,
    time::{Duration, Instant},
};

use windows_sys::Win32::{
    Foundation::HANDLE,
    System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_BASIC_ACCOUNTING_INFORMATION, JOBOBJECT_EXTENDED_LIMIT_INFORMATION,
        JobObjectBasicAccountingInformation, JobObjectExtendedLimitInformation,
        QueryInformationJobObject, SetInformationJobObject, TerminateJobObject,
    },
};

/// Owns a Windows Job Object configured to terminate all assigned processes
/// when its last handle is closed.
///
/// This is only the Job Object foundation. It does not spawn processes or
/// participate in DownloadManager task execution yet.
pub(crate) struct KillOnCloseJob {
    handle: OwnedHandle,
}

impl KillOnCloseJob {
    /// Creates a private unnamed Job Object and fails if kill-on-close cannot
    /// be configured. The owned handle closes automatically on every return
    /// path, including configuration failure.
    pub(crate) fn new() -> io::Result<Self> {
        // SAFETY: null security attributes request a non-inheritable handle;
        // null name requests a private unnamed Job Object.
        let raw_handle = unsafe { CreateJobObjectW(null(), null()) };
        if raw_handle.is_null() {
            return Err(io::Error::last_os_error());
        }

        // SAFETY: CreateJobObjectW returned a valid owned HANDLE. OwnedHandle
        // closes it exactly once when dropped.
        let handle = unsafe { OwnedHandle::from_raw_handle(raw_handle.cast()) };
        let mut limits = JOBOBJECT_EXTENDED_LIMIT_INFORMATION::default();
        limits.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;

        // SAFETY: `limits` has the exact structure and size required for
        // JobObjectExtendedLimitInformation and remains alive for the call.
        let configured = unsafe {
            SetInformationJobObject(
                handle.as_raw_handle() as HANDLE,
                JobObjectExtendedLimitInformation,
                (&limits as *const JOBOBJECT_EXTENDED_LIMIT_INFORMATION).cast(),
                size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            )
        };
        if configured == 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self { handle })
    }

    /// Adds an already-created process to this job.
    ///
    /// Future process creation must use suspended creation and call this
    /// before resuming the primary thread. This method is not a substitute for
    /// that ordering and is not wired to production process execution.
    pub(crate) fn assign_process(&self, process: RawHandle) -> io::Result<()> {
        if process.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot assign a null process handle to a Job Object",
            ));
        }

        // SAFETY: the caller supplies a live process HANDLE for the duration
        // of the call, and this object owns a live Job Object HANDLE.
        let assigned = unsafe {
            AssignProcessToJobObject(self.handle.as_raw_handle() as HANDLE, process as HANDLE)
        };
        if assigned == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    pub(super) fn terminate(&self, exit_code: u32) -> io::Result<()> {
        // SAFETY: this object owns a live Job Object handle.
        let terminated =
            unsafe { TerminateJobObject(self.handle.as_raw_handle() as HANDLE, exit_code) };
        if terminated == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Terminates every process in this private job and waits until Windows
    /// reports that the job has no active processes. Callers must not clean
    /// task-owned process files before this returns successfully.
    pub(super) fn terminate_and_wait(&self, exit_code: u32, timeout: Duration) -> io::Result<()> {
        self.terminate(exit_code)?;
        let deadline = Instant::now() + timeout;
        loop {
            let mut accounting = JOBOBJECT_BASIC_ACCOUNTING_INFORMATION::default();
            // SAFETY: the job handle is live and the output buffer is the
            // documented structure for JobObjectBasicAccountingInformation.
            let queried = unsafe {
                QueryInformationJobObject(
                    self.handle.as_raw_handle() as HANDLE,
                    JobObjectBasicAccountingInformation,
                    (&mut accounting as *mut JOBOBJECT_BASIC_ACCOUNTING_INFORMATION).cast(),
                    size_of::<JOBOBJECT_BASIC_ACCOUNTING_INFORMATION>() as u32,
                    null_mut(),
                )
            };
            if queried == 0 {
                return Err(io::Error::last_os_error());
            }
            if accounting.ActiveProcesses == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "Job Object still had active processes after termination",
                ));
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}
