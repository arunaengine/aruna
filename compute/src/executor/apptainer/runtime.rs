use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use aruna_core::compute::{AttemptPhase, BackendError};
use nix::unistd::{Pid as NixPid, setpgid, setsid};
use rustix::process::{Pid, PidfdFlags, Signal, pidfd_open, pidfd_send_signal};

use super::state::{
    ControlRecord, LaunchRecord, PayloadRecord, ProcessRecord, StatusRecord, read_json, write_json,
};

const LOG_FILE_LIMIT: u64 = 64 * 1024 * 1024;

pub fn supervisor(attempt_dir: &Path) -> Result<(), BackendError> {
    setsid().map_err(runtime_error)?;
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(attempt_dir.join("exec.lock"))
        .map_err(io_error)?;
    lock.lock().map_err(io_error)?;
    let launch: LaunchRecord = read_json(&attempt_dir.join("launch.json"))?;
    configure_cgroup(&launch)?;
    let supervisor = process_record(std::process::id())?;
    write_json(&attempt_dir.join("supervisor.json"), &supervisor)?;

    let socket_path = attempt_dir.join("launcher.sock");
    remove_socket(&socket_path)?;
    let listener = UnixListener::bind(&socket_path).map_err(io_error)?;
    listener.set_nonblocking(true).map_err(io_error)?;
    let stdout = File::create(attempt_dir.join("logs/stdout")).map_err(io_error)?;
    let stderr = File::create(attempt_dir.join("logs/stderr")).map_err(io_error)?;
    let mut child = Command::new(std::env::current_exe().map_err(io_error)?)
        .arg("payload-launcher")
        .arg(attempt_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .map_err(io_error)?;
    let process = process_record(child.id())?;
    let mut stream = accept_launcher(&listener, &mut child)?;
    let mut ready = [0u8; 1];
    stream.read_exact(&mut ready).map_err(io_error)?;
    if ready[0] != 1 {
        return Err(BackendError::Api(
            "invalid launcher readiness byte".to_string(),
        ));
    }
    write_json(
        &attempt_dir.join("payload.json"),
        &PayloadRecord {
            process,
            cgroup: launch.cgroup.clone(),
        },
    )?;
    let started_at_ms = now_ms();
    stream.write_all(&[1]).map_err(io_error)?;
    drop(stream);

    let started = Instant::now();
    let mut cancelled = false;
    let mut timed_out = false;
    let mut log_limited = false;
    let status = loop {
        if log_limit_exceeded(attempt_dir)? {
            log_limited = true;
            terminate(
                &launch.cgroup,
                Some(&read_json(&attempt_dir.join("payload.json"))?),
                Duration::from_millis(launch.stop_grace_ms),
            )?;
            break child.wait().map_err(io_error)?;
        }
        if let Some(status) = child.try_wait().map_err(io_error)? {
            break status;
        }
        let control: ControlRecord = read_json(&launch.control)?;
        if control.cancel {
            cancelled = true;
            terminate(
                &launch.cgroup,
                Some(&read_json(&attempt_dir.join("payload.json"))?),
                Duration::from_millis(launch.stop_grace_ms),
            )?;
            break child.wait().map_err(io_error)?;
        }
        if launch
            .walltime_ms
            .is_some_and(|limit| started.elapsed() >= Duration::from_millis(limit))
        {
            timed_out = true;
            terminate(
                &launch.cgroup,
                Some(&read_json(&attempt_dir.join("payload.json"))?),
                Duration::from_millis(launch.stop_grace_ms),
            )?;
            break child.wait().map_err(io_error)?;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    if !cgroup_empty(&launch.cgroup)? {
        kill_cgroup(&launch.cgroup)?;
        wait_empty(&launch.cgroup, Duration::from_secs(5))?;
    }
    let phase = if log_limited {
        AttemptPhase::Failed {
            reason: "log limit exceeded".to_string(),
        }
    } else {
        exit_phase(status, cancelled, timed_out)
    };
    write_json(
        &attempt_dir.join("status.json"),
        &StatusRecord {
            phase,
            started_at_ms: Some(started_at_ms),
            finished_at_ms: now_ms(),
        },
    )?;
    remove_socket(&socket_path)
}

pub fn launcher(attempt_dir: &Path) -> Result<(), BackendError> {
    setpgid(NixPid::from_raw(0), NixPid::from_raw(0)).map_err(runtime_error)?;
    let launch: LaunchRecord = read_json(&attempt_dir.join("launch.json"))?;
    std::fs::write(
        launch.cgroup.join("cgroup.procs"),
        std::process::id().to_string(),
    )
    .map_err(io_error)?;
    let mut stream = UnixStream::connect(attempt_dir.join("launcher.sock")).map_err(io_error)?;
    stream.write_all(&[1]).map_err(io_error)?;
    let mut release = [0u8; 1];
    if stream.read_exact(&mut release).is_err() || release[0] != 1 {
        return Err(BackendError::Cancelled);
    }
    drop(stream);

    let mut command = Command::new("apptainer");
    command
        .arg("exec")
        .args(["--containall", "--cleanenv", "--no-home", "--userns"])
        .args(["--no-privs", "--no-eval"]);
    if launch.isolated_network {
        command.args(["--net", "--network", "none"]);
    }
    for bind in &launch.binds {
        let mode = if bind.writable { "rw" } else { "ro" };
        command.arg("--bind").arg(format!(
            "{}:{}:{mode}",
            bind.host.display(),
            bind.container.display()
        ));
    }
    if let Some(workdir) = &launch.workdir {
        command.arg("--pwd").arg(workdir);
    }
    command.arg(&launch.sif).args(&launch.argv);
    use std::os::unix::process::CommandExt;
    Err(io_error(command.exec()))
}

pub fn process_record(pid: u32) -> Result<ProcessRecord, BackendError> {
    let raw = Pid::from_raw(pid as i32)
        .ok_or_else(|| BackendError::Api(format!("invalid process id {pid}")))?;
    let _pidfd = pidfd_open(raw, PidfdFlags::empty()).map_err(pidfd_error)?;
    Ok(ProcessRecord {
        pid,
        start_ticks: read_start_ticks(pid)?,
    })
}

pub fn process_live(record: &ProcessRecord) -> Result<bool, BackendError> {
    let Some(pid) = Pid::from_raw(record.pid as i32) else {
        return Ok(false);
    };
    let _pidfd = match pidfd_open(pid, PidfdFlags::empty()) {
        Ok(pidfd) => pidfd,
        Err(rustix::io::Errno::SRCH) => return Ok(false),
        Err(error) => return Err(pidfd_error(error)),
    };
    match read_start_ticks(record.pid) {
        Ok(ticks) => Ok(ticks == record.start_ticks),
        Err(BackendError::NotFound(_)) => Ok(false),
        Err(error) => Err(error),
    }
}

pub fn terminate(
    cgroup: &Path,
    payload: Option<&PayloadRecord>,
    grace: Duration,
) -> Result<(), BackendError> {
    if let Some(payload) = payload {
        signal_process(&payload.process, Signal::TERM)?;
    }
    if wait_empty(cgroup, grace).is_ok() {
        return Ok(());
    }
    kill_cgroup(cgroup)?;
    wait_empty(cgroup, Duration::from_secs(5))
}

pub fn cgroup_empty(cgroup: &Path) -> Result<bool, BackendError> {
    match std::fs::read_to_string(cgroup.join("cgroup.procs")) {
        Ok(processes) => Ok(processes.trim().is_empty()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(error) => Err(io_error(error)),
    }
}

pub fn probe_cgroup(root: &Path) -> Result<(), BackendError> {
    std::fs::create_dir_all(root).map_err(io_error)?;
    let probe = root.join(format!("aruna-health-{}-{}", std::process::id(), now_ms()));
    std::fs::create_dir(&probe).map_err(io_error)?;
    let result = (|| {
        let kill = OpenOptions::new()
            .write(true)
            .open(probe.join("cgroup.kill"))
            .map_err(io_error)?;
        (&kill).write_all(b"1").map_err(io_error)
    })();
    let remove = std::fs::remove_dir(&probe).map_err(io_error);
    result.and(remove)
}

fn configure_cgroup(launch: &LaunchRecord) -> Result<(), BackendError> {
    std::fs::create_dir(&launch.cgroup)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(error)
            }
        })
        .map_err(io_error)?;
    std::fs::write(
        launch.cgroup.join("pids.max"),
        launch.pids_limit.to_string(),
    )
    .map_err(io_error)?;
    if let Some(memory) = launch.memory_bytes {
        std::fs::write(launch.cgroup.join("memory.max"), memory.to_string()).map_err(io_error)?;
    }
    if let Some(cores) = launch.cpu_cores {
        let quota = u64::from(cores)
            .checked_mul(100_000)
            .ok_or_else(|| BackendError::InvalidSpec("CPU limit overflows".to_string()))?;
        std::fs::write(launch.cgroup.join("cpu.max"), format!("{quota} 100000"))
            .map_err(io_error)?;
    }
    Ok(())
}

fn signal_process(record: &ProcessRecord, signal: Signal) -> Result<(), BackendError> {
    let Some(pid) = Pid::from_raw(record.pid as i32) else {
        return Ok(());
    };
    let pidfd = match pidfd_open(pid, PidfdFlags::empty()) {
        Ok(pidfd) => pidfd,
        Err(rustix::io::Errno::SRCH) => return Ok(()),
        Err(error) => return Err(pidfd_error(error)),
    };
    match read_start_ticks(record.pid) {
        Ok(ticks) if ticks == record.start_ticks => {}
        Ok(_) | Err(BackendError::NotFound(_)) => return Ok(()),
        Err(error) => return Err(error),
    }
    pidfd_send_signal(pidfd, signal).map_err(pidfd_error)
}

fn read_start_ticks(pid: u32) -> Result<u64, BackendError> {
    let path = PathBuf::from(format!("/proc/{pid}/stat"));
    let stat = match std::fs::read_to_string(path) {
        Ok(stat) => stat,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Err(BackendError::NotFound(format!("process {pid}")));
        }
        Err(error) => return Err(io_error(error)),
    };
    parse_start_ticks(&stat)
}

fn parse_start_ticks(stat: &str) -> Result<u64, BackendError> {
    let end = stat
        .rfind(") ")
        .ok_or_else(|| BackendError::Api("invalid /proc stat command field".to_string()))?;
    stat[end + 2..]
        .split_whitespace()
        .nth(19)
        .ok_or_else(|| BackendError::Api("missing /proc start ticks".to_string()))?
        .parse()
        .map_err(|error| BackendError::Api(format!("invalid /proc start ticks: {error}")))
}

fn accept_launcher(
    listener: &UnixListener,
    child: &mut std::process::Child,
) -> Result<UnixStream, BackendError> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        match listener.accept() {
            Ok((stream, _)) => return Ok(stream),
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {}
            Err(error) => return Err(io_error(error)),
        }
        if let Some(status) = child.try_wait().map_err(io_error)? {
            return Err(BackendError::Api(format!(
                "payload launcher exited before readiness: {status}"
            )));
        }
        if Instant::now() >= deadline {
            return Err(BackendError::Timeout(
                "payload launcher readiness timed out".to_string(),
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn wait_empty(cgroup: &Path, timeout: Duration) -> Result<(), BackendError> {
    let deadline = Instant::now() + timeout;
    loop {
        if cgroup_empty(cgroup)? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(BackendError::Timeout(
                "Apptainer cgroup did not become empty".to_string(),
            ));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn kill_cgroup(cgroup: &Path) -> Result<(), BackendError> {
    match std::fs::write(cgroup.join("cgroup.kill"), b"1") {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn exit_phase(status: ExitStatus, cancelled: bool, timed_out: bool) -> AttemptPhase {
    if cancelled {
        AttemptPhase::Cancelled
    } else if timed_out {
        AttemptPhase::Failed {
            reason: "walltime exceeded".to_string(),
        }
    } else if let Some(code) = status.code() {
        AttemptPhase::Exited { code }
    } else {
        AttemptPhase::Failed {
            reason: "payload terminated by signal".to_string(),
        }
    }
}

fn log_limit_exceeded(attempt_dir: &Path) -> Result<bool, BackendError> {
    for name in ["stdout", "stderr"] {
        if attempt_dir
            .join("logs")
            .join(name)
            .metadata()
            .map_err(io_error)?
            .len()
            > LOG_FILE_LIMIT
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn remove_socket(path: &Path) -> Result<(), BackendError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(io_error(error)),
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn runtime_error(error: nix::Error) -> BackendError {
    BackendError::Api(format!("Apptainer runtime: {error}"))
}

fn pidfd_error(error: rustix::io::Errno) -> BackendError {
    BackendError::Api(format!("Apptainer pidfd: {error}"))
}

fn io_error(error: std::io::Error) -> BackendError {
    BackendError::Api(format!("Apptainer runtime: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_start_ticks() {
        let mut fields = vec!["S".to_string()];
        fields.extend((4..=22).map(|field| field.to_string()));
        let stat = format!("123 (name with spaces) {}", fields.join(" "));
        assert_eq!(parse_start_ticks(&stat).unwrap(), 22);
    }

    #[test]
    fn detects_log_limit() {
        let root = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("logs")).unwrap();
        File::create(root.path().join("logs/stdout")).unwrap();
        let stderr = File::create(root.path().join("logs/stderr")).unwrap();
        assert!(!log_limit_exceeded(root.path()).unwrap());

        stderr.set_len(LOG_FILE_LIMIT + 1).unwrap();
        assert!(log_limit_exceeded(root.path()).unwrap());
    }
}
