/// The smallest default memory limit, which leaves queries a 512 MiB execution budget beside the default cache.
pub const MEMORY_LIMIT_FLOOR_BYTES: usize = 1 << 30;

/// The memory limit when neither `--memory-limit` nor `RTDB_MEMORY_LIMIT_BYTES` sets one:
/// half the memory this process may use, and never below the floor.
pub fn default_memory_limit_bytes() -> usize { scaled_memory_limit(available_memory_bytes()) }

/// Half of `available`, or the floor when that is smaller or the machine's memory is unknown.
pub fn scaled_memory_limit(available: Option<u64>) -> usize {
    available.map_or(MEMORY_LIMIT_FLOOR_BYTES, |bytes| usize::try_from(bytes / 2).unwrap_or(usize::MAX).max(MEMORY_LIMIT_FLOOR_BYTES))
}

/// Physical memory, lowered to the cgroup limit a container runs under.
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
    ["/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"].iter()
        .find_map(|path| parse_cgroup_limit(&std::fs::read_to_string(path).ok()?))
}

#[cfg(not(target_os = "linux"))]
fn cgroup_memory_limit_bytes() -> Option<u64> { None }

/// Reads a cgroup memory limit file; `max` (v2) means unlimited, and v1 writes unlimited as a huge byte count.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_cgroup_limit(text: &str) -> Option<u64> {
    text.trim().parse::<u64>().ok().filter(|bytes| *bytes > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_default_limit_is_half_the_available_memory_and_never_below_the_floor() {
        assert_eq!(scaled_memory_limit(None), MEMORY_LIMIT_FLOOR_BYTES);
        assert_eq!(scaled_memory_limit(Some(512 << 20)), MEMORY_LIMIT_FLOOR_BYTES);
        assert_eq!(scaled_memory_limit(Some(2 << 30)), MEMORY_LIMIT_FLOOR_BYTES);
        assert_eq!(scaled_memory_limit(Some(64 << 30)), 32 << 30);
        assert!(default_memory_limit_bytes() >= MEMORY_LIMIT_FLOOR_BYTES);
    }

    #[test]
    fn cgroup_limits_parse_bytes_and_treat_max_as_unlimited() {
        assert_eq!(parse_cgroup_limit("8589934592\n"), Some(8 << 30));
        assert_eq!(parse_cgroup_limit("max\n"), None);
        assert_eq!(parse_cgroup_limit("0"), None);
    }
}
