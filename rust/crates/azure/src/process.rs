use std::ffi::OsStr;
use std::io::{self, Read};
use std::process::{Command, Output, Stdio};
use std::time::{Duration, Instant};
use async_trait::async_trait;
use azure_identity::Executor;
use command_group::{CommandGroup, GroupChild};

#[derive(Debug)]
pub(crate) struct JoinedExecutor;

struct ChildOwner(GroupChild);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) { let _ = self.0.kill(); }
        let _ = self.0.wait();
    }
}

#[async_trait]
impl Executor for JoinedExecutor {
    async fn run(&self, program: &OsStr, args: &[&OsStr]) -> io::Result<Output> {
        run(program, args, Duration::from_secs(30), 1024 * 1024)
    }
}

fn run(program: &OsStr, args: &[&OsStr], timeout: Duration, limit: usize) -> io::Result<Output> {
    let mut child = ChildOwner(Command::new(program).args(args).stdin(Stdio::null()).stdout(Stdio::piped()).stderr(Stdio::piped()).group_spawn()?);
    let stdout = child.0.inner().stdout.take().ok_or_else(|| io::Error::other("Credential command has no output pipe"))?;
    let stderr = child.0.inner().stderr.take().ok_or_else(|| io::Error::other("Credential command has no error pipe"))?;
    std::thread::scope(|scope| {
        let stdout = scope.spawn(move || read_bounded(stdout, limit));
        let stderr = scope.spawn(move || read_bounded(stderr, limit));
        let started = Instant::now();
        let status = loop {
            match child.0.try_wait() {
                Ok(Some(status)) if stdout.is_finished() && stderr.is_finished() => break Ok(status),
                Ok(_) if started.elapsed() < timeout => std::thread::sleep(Duration::from_millis(5)),
                Ok(_) => { let _ = child.0.kill(); let _ = child.0.wait(); break Err(io::Error::new(io::ErrorKind::TimedOut, "Credential command timed out")); }
                Err(error) => { let _ = child.0.kill(); let _ = child.0.wait(); break Err(error); }
            }
        };
        let stdout = stdout.join().map_err(|_| io::Error::other("Credential output reader failed"))?;
        let stderr = stderr.join().map_err(|_| io::Error::other("Credential error reader failed"))?;
        Ok(Output { status: status?, stdout: stdout?, stderr: stderr? })
    })
}

fn read_bounded(reader: impl Read, limit: usize) -> io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    reader.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit { return Err(io::Error::new(io::ErrorKind::InvalidData, "Credential command output exceeds its limit")); }
    Ok(bytes)
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn credential_process_output_is_bounded_and_timeout_reaps_the_child() {
        let shell = OsStr::new("/bin/sh");
        let output = run(shell, &[OsStr::new("-c"), OsStr::new("printf token")], Duration::from_secs(1), 16).unwrap();
        assert_eq!(output.stdout, b"token");
        assert!(run(shell, &[OsStr::new("-c"), OsStr::new("printf toolong")], Duration::from_secs(1), 4).is_err());
        assert_eq!(run(shell, &[OsStr::new("-c"), OsStr::new("while :; do :; done")], Duration::from_millis(20), 16).unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert_eq!(run(shell, &[OsStr::new("-c"), OsStr::new("sleep 10 & wait")], Duration::from_millis(20), 16).unwrap_err().kind(), io::ErrorKind::TimedOut);
    }
}
