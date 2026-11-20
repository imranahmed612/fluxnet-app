use std::{
    collections::HashSet,
    ffi::OsStr,
    io,
    mem::size_of,
    os::windows::{
        ffi::OsStrExt,
        io::{AsRawHandle, FromRawHandle, IntoRawHandle, OwnedHandle, RawHandle},
    },
    path::{Path, PathBuf},
    ptr::{null, null_mut},
    time::Duration,
};

use windows_sys::Win32::{
    Foundation::{
        CloseHandle, ERROR_INSUFFICIENT_BUFFER, GENERIC_READ, GENERIC_WRITE, GetLastError, HANDLE,
        HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation, WAIT_OBJECT_0,
    },
    Security::SECURITY_ATTRIBUTES,
    Storage::FileSystem::{
        CreateFileW, FILE_ATTRIBUTE_NORMAL, FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
    },
    System::Pipes::CreatePipe,
    System::Threading::{
        CREATE_NO_WINDOW, CREATE_SUSPENDED, CREATE_UNICODE_ENVIRONMENT, CreateProcessW,
        DeleteProcThreadAttributeList, EXTENDED_STARTUPINFO_PRESENT, INFINITE,
        InitializeProcThreadAttributeList, PROC_THREAD_ATTRIBUTE_HANDLE_LIST, PROCESS_INFORMATION,
        ResumeThread, STARTF_USESTDHANDLES, STARTUPINFOEXW, TerminateProcess,
        UpdateProcThreadAttribute, WaitForSingleObject,
    },
};

use super::windows_job::KillOnCloseJob;

const FIXTURE_TERMINATION_CODE: u32 = 0xC000_013A;
const MAX_COMMAND_LINE_UNITS: usize = 32_767;

/// One explicitly configured process invocation. The environment is replaced,
/// not inherited; callers must supply every variable the child is allowed to
/// observe.
#[allow(dead_code)]
pub(crate) struct ProcessSpec {
    pub(crate) executable: PathBuf,
    pub(crate) args: Vec<std::ffi::OsString>,
    pub(crate) working_directory: PathBuf,
    pub(crate) environment: Vec<(std::ffi::OsString, std::ffi::OsString)>,
    pub(crate) timeout: Duration,
    pub(crate) stdout_limit: usize,
    pub(crate) stderr_limit: usize,
}

/// A process that was created suspended, assigned to a kill-on-close Job
/// Object, and only then resumed. Dropping it closes the job and terminates
/// the contained process tree.
#[allow(dead_code)]
pub(crate) struct ContainedProcess {
    // Keep the job alive until cleanup; it is dropped before the process handle.
    pub(super) _job: KillOnCloseJob,
    pub(super) _process: OwnedHandle,
    #[allow(dead_code)]
    pub(super) process_id: u32,
    pub(super) stdout: std::fs::File,
    pub(super) stderr: std::fs::File,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum FailurePoint {
    Create,
    Assign,
    Resume,
}

#[cfg(test)]
pub(super) fn spawn_with_failure(
    spec: &ProcessSpec,
    failure: FailurePoint,
    created_pid: &mut Option<u32>,
) -> io::Result<ContainedProcess> {
    spawn_inner(spec, Some(failure), created_pid)
}

/// Launches one contained process. There is deliberately no uncontained
/// fallback if Job Object setup, assignment, or resume fails.
#[allow(dead_code)]
pub(crate) fn spawn_contained(spec: &ProcessSpec) -> io::Result<ContainedProcess> {
    spawn_inner(spec, None, &mut None)
}

fn spawn_inner(
    spec: &ProcessSpec,
    failure: Option<FailurePoint>,
    created_pid: &mut Option<u32>,
) -> io::Result<ContainedProcess> {
    validate_spec(spec)?;
    let executable = encode_path(&spec.executable)?;
    let current_directory = encode_path(&spec.working_directory)?;
    let mut command_line = encode_command_line(&spec.executable, &spec.args)?;
    let environment = encode_environment(&spec.environment)?;

    // Create containment before any child exists. A Job Object setup failure
    // therefore cannot leave a process running outside containment.
    let job = KillOnCloseJob::new()?;
    let null_stdio = create_inheritable_null_handle()?;
    let (stdout_read, stdout_write) = create_pipe()?;
    let (stderr_read, stderr_write) = create_pipe()?;
    let inherited_handles = [
        null_stdio.as_raw_handle() as HANDLE,
        stdout_write.as_raw_handle() as HANDLE,
        stderr_write.as_raw_handle() as HANDLE,
    ];
    let attribute_list = ProcThreadAttributeList::new(&inherited_handles)?;

    if failure == Some(FailurePoint::Create) {
        return Err(injected_failure("CreateProcessW"));
    }

    let mut startup = STARTUPINFOEXW::default();
    startup.StartupInfo.cb = size_of::<STARTUPINFOEXW>() as u32;
    startup.StartupInfo.dwFlags = STARTF_USESTDHANDLES;
    startup.StartupInfo.hStdInput = inherited_handles[0];
    startup.StartupInfo.hStdOutput = inherited_handles[1];
    startup.StartupInfo.hStdError = inherited_handles[2];
    startup.lpAttributeList = attribute_list.as_ptr();

    let mut process_information = PROCESS_INFORMATION::default();
    let creation_flags = CREATE_SUSPENDED
        | CREATE_UNICODE_ENVIRONMENT
        | EXTENDED_STARTUPINFO_PRESENT
        | CREATE_NO_WINDOW;

    // SAFETY: all strings/blocks remain alive and NUL-terminated for this
    // synchronous call. The explicit handle list limits inheritance to the
    // single NUL stdio handle. Creation is suspended and uses an explicit app
    // path, cwd and environment block.
    let created = unsafe {
        CreateProcessW(
            executable.as_ptr(),
            command_line.as_mut_ptr(),
            null(),
            null(),
            1,
            creation_flags,
            environment.as_ptr().cast(),
            current_directory.as_ptr(),
            &startup.StartupInfo,
            &mut process_information,
        )
    };
    if created == 0 {
        return Err(io::Error::last_os_error());
    }

    // Own both handles immediately. If an impossible null handle is returned
    // despite success, close any valid handle and fail without resuming.
    if process_information.hProcess.is_null() || process_information.hThread.is_null() {
        // SAFETY: these are the handles returned by CreateProcessW; null values
        // are skipped because they do not identify owned kernel handles.
        unsafe {
            if !process_information.hThread.is_null() {
                CloseHandle(process_information.hThread);
            }
            if !process_information.hProcess.is_null() {
                TerminateProcess(process_information.hProcess, FIXTURE_TERMINATION_CODE);
                WaitForSingleObject(process_information.hProcess, INFINITE);
                CloseHandle(process_information.hProcess);
            }
        }
        return Err(io::Error::other(
            "CreateProcessW returned success without valid process and thread handles",
        ));
    }

    // SAFETY: CreateProcessW returned these two owned kernel handles.
    let process_handle =
        unsafe { OwnedHandle::from_raw_handle(process_information.hProcess.cast()) };
    // SAFETY: CreateProcessW returned these two owned kernel handles.
    let thread_handle = unsafe { OwnedHandle::from_raw_handle(process_information.hThread.cast()) };
    *created_pid = Some(process_information.dwProcessId);
    let mut suspended = SuspendedProcessGuard::new(process_handle, thread_handle, job);

    // Remove the temporary parent's inherit bit as soon as CreateProcessW has
    // Clear inheritance on every parent-owned handle as soon as the child has
    // copied its explicit handle list. Failure terminates the suspended child.
    for handle in inherited_handles {
        // SAFETY: each handle is owned and open in this scope.
        if unsafe { SetHandleInformation(handle, HANDLE_FLAG_INHERIT, 0) } == 0 {
            return Err(suspended.abort_with(io::Error::last_os_error()));
        }
    }
    drop(null_stdio);
    drop(stdout_write);
    drop(stderr_write);

    if failure == Some(FailurePoint::Assign) {
        return Err(suspended.abort_with(injected_failure("AssignProcessToJobObject")));
    }
    if let Err(error) = suspended.job().assign_process(suspended.process_raw()) {
        return Err(suspended.abort_with(error));
    }
    suspended.mark_assigned();

    if failure == Some(FailurePoint::Resume) {
        return Err(suspended.abort_with(injected_failure("ResumeThread")));
    }

    // SAFETY: the primary thread handle is owned by the guard and was created
    // with CREATE_SUSPENDED. It is resumed only after successful job assignment.
    let previous_suspend_count = unsafe { ResumeThread(suspended.thread_raw()) };
    if previous_suspend_count == u32::MAX {
        return Err(suspended.abort_with(io::Error::last_os_error()));
    }
    if previous_suspend_count != 1 {
        return Err(suspended.abort_with(io::Error::other(
            "primary thread had an unexpected suspend count",
        )));
    }

    Ok(suspended.into_contained(process_information.dwProcessId, stdout_read, stderr_read))
}

fn create_pipe() -> io::Result<(OwnedHandle, OwnedHandle)> {
    let mut read = null_mut();
    let mut write = null_mut();
    let mut security = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: 1,
    };
    // SAFETY: output pointers and security attributes remain valid for the call.
    if unsafe { CreatePipe(&mut read, &mut write, &mut security, 0) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: CreatePipe returned two owned handles.
    Ok(unsafe {
        (
            OwnedHandle::from_raw_handle(read.cast()),
            OwnedHandle::from_raw_handle(write.cast()),
        )
    })
}

fn validate_spec(spec: &ProcessSpec) -> io::Result<()> {
    if !spec.executable.is_absolute() || !spec.working_directory.is_absolute() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "executable and working directory must be absolute paths",
        ));
    }
    if !spec.working_directory.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "working directory must exist and be a directory",
        ));
    }
    if spec
        .executable
        .as_os_str()
        .encode_wide()
        .any(|unit| unit == 0)
        || spec
            .args
            .iter()
            .any(|arg| arg.encode_wide().any(|unit| unit == 0))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "executable and arguments cannot contain NUL characters",
        ));
    }
    for (key, value) in &spec.environment {
        let key = key.encode_wide().collect::<Vec<_>>();
        if key.is_empty() || key.contains(&(b'=' as u16)) || key.contains(&0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "environment variable names must be nonempty and cannot contain '=' or NUL",
            ));
        }
        if value.encode_wide().any(|unit| unit == 0) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "environment variable values cannot contain NUL",
            ));
        }
    }
    Ok(())
}

fn encode_path(path: &Path) -> io::Result<Vec<u16>> {
    let mut wide = path.as_os_str().encode_wide().collect::<Vec<_>>();
    if wide.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows paths cannot contain NUL characters",
        ));
    }
    wide.push(0);
    Ok(wide)
}

fn encode_command_line(executable: &Path, args: &[std::ffi::OsString]) -> io::Result<Vec<u16>> {
    let mut command_line = quote_windows_argument(executable.as_os_str())?;
    for arg in args {
        command_line.push(b' ' as u16);
        command_line.extend(quote_windows_argument(arg)?);
    }
    if command_line.len() + 1 > MAX_COMMAND_LINE_UNITS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows command line exceeds the documented length limit",
        ));
    }
    command_line.push(0);
    Ok(command_line)
}

/// Quotes one argument according to the Windows CRT backslash/quote rules.
pub(super) fn quote_windows_argument(argument: &OsStr) -> io::Result<Vec<u16>> {
    let units = argument.encode_wide().collect::<Vec<_>>();
    if units.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Windows arguments cannot contain NUL characters",
        ));
    }
    let needs_quotes = units.is_empty()
        || units
            .iter()
            .any(|unit| matches!(*unit, 0x09..=0x0D | 0x20 | 0x22));
    if !needs_quotes {
        return Ok(units);
    }

    let mut quoted = Vec::with_capacity(units.len() + 2);
    quoted.push(b'"' as u16);
    let mut backslashes = 0usize;
    for unit in units {
        if unit == b'\\' as u16 {
            backslashes += 1;
            continue;
        }
        if unit == b'"' as u16 {
            quoted.extend(std::iter::repeat_n(b'\\' as u16, backslashes * 2 + 1));
            quoted.push(unit);
        } else {
            quoted.extend(std::iter::repeat_n(b'\\' as u16, backslashes));
            quoted.push(unit);
        }
        backslashes = 0;
    }
    quoted.extend(std::iter::repeat_n(b'\\' as u16, backslashes * 2));
    quoted.push(b'"' as u16);
    Ok(quoted)
}

fn encode_environment(
    environment: &[(std::ffi::OsString, std::ffi::OsString)],
) -> io::Result<Vec<u16>> {
    let mut entries = Vec::with_capacity(environment.len());
    let mut seen = HashSet::with_capacity(environment.len());
    for (key, value) in environment {
        let key_wide = key.encode_wide().collect::<Vec<_>>();
        let value_wide = value.encode_wide().collect::<Vec<_>>();
        let comparison_key = key.to_string_lossy().to_uppercase();
        if !seen.insert(comparison_key) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "environment variable names must be unique ignoring case",
            ));
        }
        entries.push((key_wide, value_wide, key.to_string_lossy().to_uppercase()));
    }
    entries.sort_by(|left, right| left.2.cmp(&right.2));

    let mut block = Vec::new();
    for (key, value, _) in entries {
        block.extend(key);
        block.push(b'=' as u16);
        block.extend(value);
        block.push(0);
    }
    block.push(0);
    if environment.is_empty() {
        block.push(0);
    }
    Ok(block)
}

fn create_inheritable_null_handle() -> io::Result<OwnedHandle> {
    let mut null_path = "NUL".encode_utf16().collect::<Vec<_>>();
    null_path.push(0);
    let mut security = SECURITY_ATTRIBUTES {
        nLength: size_of::<SECURITY_ATTRIBUTES>() as u32,
        lpSecurityDescriptor: null_mut(),
        bInheritHandle: 1,
    };

    // SAFETY: path and security attributes remain live for this call. The
    // returned inheritable handle is immediately wrapped in OwnedHandle.
    let handle = unsafe {
        CreateFileW(
            null_path.as_ptr(),
            GENERIC_READ | GENERIC_WRITE,
            FILE_SHARE_READ | FILE_SHARE_WRITE,
            &mut security,
            OPEN_EXISTING,
            FILE_ATTRIBUTE_NORMAL,
            null_mut(),
        )
    };
    if handle == INVALID_HANDLE_VALUE {
        return Err(io::Error::last_os_error());
    }

    // SAFETY: CreateFileW returned a valid owned HANDLE.
    Ok(unsafe { OwnedHandle::from_raw_handle(handle.cast()) })
}

struct ProcThreadAttributeList {
    storage: Vec<usize>,
    initialized: bool,
}

impl ProcThreadAttributeList {
    fn new(handles: &[HANDLE]) -> io::Result<Self> {
        if handles.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the inherited handle list cannot be empty",
            ));
        }

        let mut required_bytes = 0usize;
        // SAFETY: null is the documented sizing call. No list is initialized
        // or owned on this call.
        let sizing_result =
            unsafe { InitializeProcThreadAttributeList(null_mut(), 1, 0, &mut required_bytes) };
        if sizing_result != 0
            || required_bytes == 0
            || unsafe { GetLastError() } != ERROR_INSUFFICIENT_BUFFER
        {
            return Err(io::Error::last_os_error());
        }

        let mut storage = vec![0usize; required_bytes.div_ceil(size_of::<usize>())];
        let list = storage.as_mut_ptr().cast();
        // SAFETY: storage is sufficiently sized and aligned for the opaque
        // attribute-list structure per the sizing call above.
        if unsafe { InitializeProcThreadAttributeList(list, 1, 0, &mut required_bytes) } == 0 {
            return Err(io::Error::last_os_error());
        }
        let mut result = Self {
            storage,
            initialized: true,
        };

        // SAFETY: the list is initialized; `handles` remains alive for the
        // call and contains only handles deliberately marked inheritable.
        let updated = unsafe {
            UpdateProcThreadAttribute(
                result.as_mut_ptr(),
                0,
                PROC_THREAD_ATTRIBUTE_HANDLE_LIST as usize,
                handles.as_ptr().cast(),
                size_of::<HANDLE>() * handles.len(),
                null_mut(),
                null(),
            )
        };
        if updated == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(result)
    }

    fn as_ptr(&self) -> *mut core::ffi::c_void {
        self.storage.as_ptr().cast_mut().cast()
    }

    fn as_mut_ptr(&mut self) -> *mut core::ffi::c_void {
        self.storage.as_mut_ptr().cast()
    }
}

impl Drop for ProcThreadAttributeList {
    fn drop(&mut self) {
        if self.initialized {
            // SAFETY: this list was successfully initialized and its backing
            // storage remains valid until after this call.
            unsafe { DeleteProcThreadAttributeList(self.as_mut_ptr()) };
            self.initialized = false;
        }
    }
}

struct SuspendedProcessGuard {
    process: Option<OwnedHandle>,
    thread: Option<OwnedHandle>,
    job: Option<KillOnCloseJob>,
    assigned: bool,
    armed: bool,
}

impl SuspendedProcessGuard {
    fn new(process: OwnedHandle, thread: OwnedHandle, job: KillOnCloseJob) -> Self {
        Self {
            process: Some(process),
            thread: Some(thread),
            job: Some(job),
            assigned: false,
            armed: true,
        }
    }

    fn process_raw(&self) -> RawHandle {
        self.process
            .as_ref()
            .expect("guard owns a process handle")
            .as_raw_handle()
    }

    fn thread_raw(&self) -> HANDLE {
        self.thread
            .as_ref()
            .expect("guard owns a primary thread handle")
            .as_raw_handle() as HANDLE
    }

    fn job(&self) -> &KillOnCloseJob {
        self.job.as_ref().expect("guard owns a Job Object")
    }

    fn mark_assigned(&mut self) {
        self.assigned = true;
    }

    fn abort_with(&mut self, original: io::Error) -> io::Error {
        match self.terminate_and_wait() {
            Ok(()) => original,
            Err(cleanup) => io::Error::new(
                cleanup.kind(),
                format!("{original}; suspended-process cleanup also failed: {cleanup}"),
            ),
        }
    }

    fn terminate_and_wait(&mut self) -> io::Result<()> {
        if !self.armed {
            return Ok(());
        }
        let process = self.process_raw() as HANDLE;
        let termination = if self.assigned {
            self.job().terminate(FIXTURE_TERMINATION_CODE)
        } else {
            // Before successful job assignment the process is still suspended,
            // so terminating the root cannot leave an executing descendant.
            // SAFETY: the guard owns a valid process handle.
            let terminated = unsafe { TerminateProcess(process, FIXTURE_TERMINATION_CODE) };
            if terminated == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(())
            }
        };

        let termination_error = termination.err();
        if termination_error.is_some() {
            // Best-effort root termination is safe here: before assignment the
            // primary thread has never run; after assignment job close below
            // still kills any remaining job members.
            // SAFETY: the guard owns a valid process handle.
            unsafe { TerminateProcess(process, FIXTURE_TERMINATION_CODE) };
        }

        // SAFETY: waiting on the owned process handle until termination is
        // required before releasing task/child resources.
        let wait = unsafe { WaitForSingleObject(process, INFINITE) };
        if wait != WAIT_OBJECT_0 {
            return Err(io::Error::last_os_error());
        }

        self.armed = false;
        self.thread.take();
        self.process.take();
        self.job.take(); // kill-on-close also cleans any remaining descendants.
        if let Some(error) = termination_error {
            return Err(error);
        }
        Ok(())
    }

    fn into_contained(
        mut self,
        process_id: u32,
        stdout: OwnedHandle,
        stderr: OwnedHandle,
    ) -> ContainedProcess {
        self.armed = false;
        drop(self.thread.take());
        ContainedProcess {
            _job: self.job.take().expect("guard owns a Job Object"),
            _process: self.process.take().expect("guard owns a process handle"),
            process_id,
            // SAFETY: the parent owns the non-inheritable read ends returned by CreatePipe.
            stdout: unsafe { std::fs::File::from_raw_handle(stdout.into_raw_handle()) },
            // SAFETY: the parent owns the non-inheritable read ends returned by CreatePipe.
            stderr: unsafe { std::fs::File::from_raw_handle(stderr.into_raw_handle()) },
        }
    }
}

impl Drop for SuspendedProcessGuard {
    fn drop(&mut self) {
        let _ = self.terminate_and_wait();
    }
}

fn injected_failure(operation: &str) -> io::Error {
    io::Error::other(format!("injected {operation} failure"))
}
