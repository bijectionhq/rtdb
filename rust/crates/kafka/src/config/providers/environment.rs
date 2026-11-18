use std::{
    collections::{BTreeMap, BTreeSet, btree_map::Entry},
    ffi::OsString,
    sync::Arc,
};

use rtdb_expr::JavaFullMatcher;
use rtdb_types::{AllocationScope, Anomaly, MemoryBudget, Result, java_text::JavaText};

use super::{Values, add, allocation, invalid};

pub(super) struct Environment {
    filtered: BTreeMap<String, OsString>,
    _allocation: AllocationScope,
}

impl Environment {
    pub(super) fn capture(pattern: Option<&str>, memory: &Arc<MemoryBudget>) -> Result<Self> {
        let allocation = AllocationScope::new(memory)?;
        Self::filter(std::env::vars_os(), pattern, allocation)
    }

    fn filter(
        captured: impl IntoIterator<Item = (OsString, OsString)>,
        pattern: Option<&str>,
        allocation: AllocationScope,
    ) -> Result<Self> {
        let mut matcher = match pattern {
            None | Some(".*") => None,
            Some(pattern) => Some(JavaFullMatcher::compile(JavaText::from_utf8(pattern), allocation.memory_budget())?),
        };
        let mut filtered = BTreeMap::new();
        for (name, value) in captured {
            let name = name.into_string().map_err(|_| Anomaly::unsupported(
                "xtdb.kafka/config-provider-environment-encoding", "Kafka environment provider requires the native process name encoding",
            ))?;
            let allowed = match matcher.as_mut() {
                Some(matcher) => matcher.matches(JavaText::from_utf8(&name))?,
                None => !name.contains(['\n', '\r', '\u{85}', '\u{2028}', '\u{2029}']),
            };
            if !allowed { continue; }
            let bytes = add(add(name.capacity(), value.capacity())?, 512)?;
            if let Entry::Vacant(slot) = filtered.entry(name) {
                allocation.reserve(bytes)?;
                slot.insert(value);
            }
        }
        Ok(Self { filtered, _allocation: allocation })
    }

    pub(super) fn get<'key>(
        &self,
        path: &str,
        keys: &BTreeSet<&'key str>,
        reserve: &mut impl FnMut(usize) -> Result<()>,
    ) -> Result<Values<'key>> {
        if !path.is_empty() { return Err(invalid("Kafka environment ConfigProvider requires an empty path")); }
        let mut values = Values::new();
        for &key in keys {
            if let Some(host) = self.filtered.get(key) {
                let text = host.to_str().ok_or_else(|| Anomaly::unsupported(
                    "xtdb.kafka/config-provider-environment-encoding", "Kafka environment provider requires the native process value encoding",
                ))?;
                reserve(add(text.len(), 512)?)?;
                let mut value = String::new();
                value.try_reserve_exact(text.len()).map_err(|_| allocation())?;
                value.push_str(text);
                values.insert(key, value);
            }
        }
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs<const N: usize>(input: [(&str, &str); N]) -> [(OsString, OsString); N] {
        input.map(|(name, value)| (name.into(), value.into()))
    }

    #[test]
    fn full_allowlist_filters_the_snapshot_and_get_selects_exact_names() {
        let memory = MemoryBudget::new(1 << 29);
        let environment = Environment::filter(
            pairs([("KAFKA_TOKEN", "secret"), ("KAFKA_NAME", "literal\nvalue"), ("OTHER_NAME", "different"), ("KAFKA_NAME_NO", "no")]),
            Some("KAFKA_N|(?i:kafka_(?:name|token))"),
            AllocationScope::new(&memory).unwrap(),
        ).unwrap();
        assert_eq!(environment.filtered.keys().map(String::as_str).collect::<Vec<_>>(), ["KAFKA_NAME", "KAFKA_TOKEN"]);
        let retained = memory.used_bytes();
        assert!(retained > 4096);
        let keys = BTreeSet::from(["KAFKA_NAME", "kafka_name", "KAFKA_NAME_NO"]);
        let mut sizes = Vec::new();
        let resolved = environment.get("", &keys, &mut |size| { sizes.push(size); Ok(()) }).unwrap();
        assert_eq!(resolved, BTreeMap::from([("KAFKA_NAME", String::from("literal\nvalue"))]));
        assert_eq!(sizes, [525]);
        assert_eq!(memory.used_bytes(), retained);
        drop(environment);
        assert_eq!(memory.used_bytes(), 0);
    }

    #[test]
    fn default_excludes_java_dot_line_terminators_and_invalid_path_precedes_get() {
        let input = pairs([("one", "1"), ("LF\n", "2"), ("CR\r", "3"), ("NEL\u{85}", "4"), ("LS\u{2028}", "5"), ("PS\u{2029}", "6")]);
        let memory = MemoryBudget::new(1 << 25);
        let environment = Environment::filter(input, None, AllocationScope::new(&memory).unwrap()).unwrap();
        let keys = BTreeSet::from(["one", "LF\n", "CR\r", "NEL\u{85}", "LS\u{2028}", "PS\u{2029}"]);
        assert!(environment.get("folder", &keys, &mut |_| panic!("invalid path demands no result allocation")).is_err());
        let observed = environment.get("", &keys, &mut |_| Ok(())).unwrap();
        assert_eq!(observed.keys().copied().collect::<Vec<_>>(), ["one"]);
    }

    #[test]
    fn invalid_patterns_and_admission_fail_before_publishing_a_snapshot() {
        let memory = MemoryBudget::new(1 << 28);
        let input = pairs([("KAFKA_TOKEN", "secret")]);
        let syntax = Environment::filter(input.clone(), Some("["), AllocationScope::new(&memory).unwrap());
        assert!(syntax.is_err());
        let tight = memory.scope_with_limit(4096);
        let denied = Environment::filter(input, None, AllocationScope::new(&tight).unwrap());
        assert_eq!(denied.err().unwrap().code, "rtdb/memory-budget-exceeded");
        assert_eq!(memory.used_bytes(), 0);
    }
}
