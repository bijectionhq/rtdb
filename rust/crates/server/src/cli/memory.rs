use std::path::Path;

/// The memory limit when the memory this process may use is unknown.
pub const UNKNOWN_MEMORY_LIMIT_BYTES: usize = 1 << 30;

/// The memory limit when neither `--memory-limit` nor `RTDB_MEMORY_LIMIT_BYTES` sets one:
/// half the memory this process may use, as the JVM's default heap is a fraction of it.
pub fn default_memory_limit_bytes() -> usize { scaled_memory_limit(available_memory_bytes()) }

/// Half of `available`, or [`UNKNOWN_MEMORY_LIMIT_BYTES`] when the process's memory is unknown.
/// A known limit is never raised: a budget above what the kernel allows is killed before it refuses.
pub fn scaled_memory_limit(available: Option<u64>) -> usize {
    available.map_or(UNKNOWN_MEMORY_LIMIT_BYTES, |bytes| usize::try_from(bytes / 2).unwrap_or(usize::MAX))
}

/// Physical memory, lowered to the cgroup limit the process runs under.
fn available_memory_bytes() -> Option<u64> {
    match (physical_memory_bytes(), cgroup_memory_limit_bytes()) {
        (Some(physical), Some(cgroup)) => Some(physical.min(cgroup)),
        (physical, cgroup) => physical.or(cgroup),
    }
}

#[cfg(unix)]
fn physical_memory_bytes() -> Option<u64> {
    // SAFETY: `sysconf` reads a system constant and has no preconditions.
    let (pages, page_size) = unsafe { (libc::sysconf(libc::_SC_PHYS_PAGES), libc::sysconf(libc::_SC_PAGESIZE)) };
    let (pages, page_size) = (u64::try_from(pages).ok()?, u64::try_from(page_size).ok()?);
    pages.checked_mul(page_size).filter(|bytes| *bytes > 0)
}

#[cfg(not(unix))]
fn physical_memory_bytes() -> Option<u64> { None }

#[cfg(target_os = "linux")]
fn cgroup_memory_limit_bytes() -> Option<u64> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").ok()?;
    cgroup_memory_limit(&membership, Path::new("/sys/fs/cgroup"), &|path| std::fs::read_to_string(path).ok())
}

#[cfg(not(target_os = "linux"))]
fn cgroup_memory_limit_bytes() -> Option<u64> { None }

/// The tightest memory limit on the process's own cgroup or any of its ancestors, as `membership` (`/proc/self/cgroup`) names them:
/// `memory.max` along the unified (v2) path below `root`, and `memory.limit_in_bytes` along the v1 `memory` controller's path below `root/memory`.
/// A limit set on a parent binds its children, and inside a cgroup namespace the process's own cgroup is the mount root.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn cgroup_memory_limit(membership: &str, root: &Path, read: &impl Fn(&Path) -> Option<String>) -> Option<u64> {
    membership.lines()
        .filter_map(|line| {
            let mut fields = line.splitn(3, ':');
            let (_, controllers, path) = (fields.next()?, fields.next()?, fields.next()?);
            match controllers {
                "" => Some((root.to_path_buf(), "memory.max", path)),
                _ if controllers.split(',').any(|controller| controller == "memory") => Some((root.join("memory"), "memory.limit_in_bytes", path)),
                _ => None,
            }
        })
        .flat_map(|(mount, file, path)| Path::new(path).ancestors()
            .filter_map(|directory| read(&mount.join(directory.strip_prefix("/").ok()?).join(file)))
            .filter_map(|text| parse_cgroup_limit(&text))
            .collect::<Vec<_>>())
        .min()
}

/// Reads a cgroup memory limit file; `max` (v2) means unlimited, and v1 writes unlimited as a huge byte count.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_cgroup_limit(text: &str) -> Option<u64> {
    text.trim().parse::<u64>().ok().filter(|bytes| *bytes > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    #[test]
    fn the_default_limit_is_half_the_available_memory_even_below_one_gibibyte() {
        assert_eq!(scaled_memory_limit(None), UNKNOWN_MEMORY_LIMIT_BYTES);
        assert_eq!(scaled_memory_limit(Some(512 << 20)), 256 << 20);
        assert_eq!(scaled_memory_limit(Some(2 << 30)), 1 << 30);
        assert_eq!(scaled_memory_limit(Some(64 << 30)), 32 << 30);
    }

    #[test]
    fn cgroup_limits_parse_bytes_and_treat_max_as_unlimited() {
        assert_eq!(parse_cgroup_limit("8589934592\n"), Some(8 << 30));
        assert_eq!(parse_cgroup_limit("max\n"), None);
        assert_eq!(parse_cgroup_limit("0"), None);
    }

    fn files(entries: &[(&str, &str)]) -> impl Fn(&Path) -> Option<String> {
        let files = entries.iter().map(|(path, text)| (std::path::PathBuf::from(path), (*text).to_owned())).collect::<BTreeMap<_, _>>();
        move |path: &Path| files.get(path).cloned()
    }

    #[test]
    fn the_process_s_own_unified_cgroup_and_its_ancestors_bound_the_limit() {
        let root = Path::new("/sys/fs/cgroup");
        let read = files(&[
            ("/sys/fs/cgroup/memory.max", "max\n"),
            ("/sys/fs/cgroup/kubepods/memory.max", "4294967296\n"),
            ("/sys/fs/cgroup/kubepods/pod1/memory.max", "max\n"),
            ("/sys/fs/cgroup/kubepods/pod1/app/memory.max", "536870912\n"),
            ("/sys/fs/cgroup/other/memory.max", "1048576\n"),
        ]);
        assert_eq!(cgroup_memory_limit("0::/kubepods/pod1/app\n", root, &read), Some(512 << 20));
        assert_eq!(cgroup_memory_limit("0::/kubepods/pod1\n", root, &read), Some(4 << 30));
        assert_eq!(cgroup_memory_limit("0::/\n", root, &files(&[("/sys/fs/cgroup/memory.max", "268435456\n")])), Some(256 << 20));
        assert_eq!(cgroup_memory_limit("0::/\n", root, &read), None);
    }

    #[test]
    fn the_v1_memory_controller_path_bounds_the_limit() {
        let root = Path::new("/sys/fs/cgroup");
        let read = files(&[
            ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "9223372036854771712\n"),
            ("/sys/fs/cgroup/memory/docker/abc/memory.limit_in_bytes", "805306368\n"),
            ("/sys/fs/cgroup/cpu/docker/abc/memory.limit_in_bytes", "1\n"),
        ]);
        let membership = "12:cpu,cpuacct:/docker/abc\n4:memory:/docker/abc\n0::/docker/abc\n";
        assert_eq!(cgroup_memory_limit(membership, root, &read), Some(768 << 20));
        assert_eq!(cgroup_memory_limit("4:memory:/elsewhere\n", root, &read), Some(9223372036854771712));
        assert_eq!(cgroup_memory_limit("", root, &read), None);
    }
}
