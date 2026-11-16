use std::{collections::{BTreeMap, VecDeque}, sync::Arc, time::{Duration, Instant}};
use rtdb_types::Result;
use super::super::{converter::option_bool, error};

pub(super) enum Selection { Writer, Latest, Metadata(BTreeMap<String, String>) }
impl Selection {
    pub fn parse(options: &BTreeMap<String, String>) -> Result<Self> {
        let latest = option_bool(options, "use.latest.version", false)?;
        if let Some(text) = options.get("use.latest.with.metadata") {
            let metadata = metadata(text)?;
            return Ok(if metadata.is_empty() { Self::Writer } else { Self::Metadata(metadata) });
        }
        Ok(if latest { Self::Latest } else { Self::Writer })
    }
    pub fn key(&self, subject: &str, writer: u32) -> Option<Key> {
        match self { Self::Writer => None, Self::Latest => Some(Key::Latest { subject: subject.into(), writer }), Self::Metadata(_) => Some(Key::Metadata(subject.into())) }
    }
    pub fn request(&self, subject: &str) -> Option<(Vec<String>, Vec<(String, String)>)> {
        match self {
            Self::Writer => None,
            Self::Latest => Some((vec!["subjects".into(), subject.into(), "versions".into(), "latest".into()], Vec::new())),
            Self::Metadata(metadata) => {
                let mut query = Vec::with_capacity(metadata.len().saturating_mul(2).saturating_add(1));
                for (key, value) in metadata { query.push(("key".into(), key.clone())); query.push(("value".into(), value.clone())); }
                query.push(("deleted".into(), "true".into()));
                Some((vec!["subjects".into(), subject.into(), "metadata".into()], query))
            }
        }
    }
    pub fn request_bound(&self, subject: &str) -> Result<usize> {
        let mut bytes = subject.len().checked_mul(8).and_then(|bytes| bytes.checked_add(4096)).ok_or_else(overflow)?;
        if let Self::Metadata(metadata) = self {
            for (key, value) in metadata {
                bytes = bytes.checked_add(key.len().checked_add(value.len()).and_then(|bytes| bytes.checked_add(512)).and_then(|bytes| bytes.checked_mul(16)).ok_or_else(overflow)?).ok_or_else(overflow)?;
            }
        }
        Ok(bytes)
    }
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum Key { Latest { subject: String, writer: u32 }, Metadata(String) }
struct Entry<T> { value: Arc<T>, written: Instant }
pub(super) struct LatestCache<T> { entries: BTreeMap<Key, Entry<T>>, order: VecDeque<Key>, capacity: usize, ttl: Option<Duration> }
impl<T> LatestCache<T> {
    pub fn open(options: &BTreeMap<String, String>) -> Result<Self> {
        let integer = |name, default| options.get(name).map(|text| text.trim_matches(|ch| ch <= ' ').parse::<i32>().map_err(|cause| error("invalid-plugin-config", cause))).transpose().map(|value| value.unwrap_or(default));
        let capacity = usize::try_from(integer("latest.cache.size", 1000)?).map_err(|_| error("invalid-plugin-config", "Latest schema cache size cannot be negative"))?;
        let seconds = integer("latest.cache.ttl.sec", -1)?;
        Ok(Self { entries: BTreeMap::new(), order: VecDeque::new(), capacity, ttl: (seconds >= 0).then(|| Duration::from_secs(seconds as u64)) })
    }
    pub fn get(&mut self, key: &Key, now: Instant) -> Option<Arc<T>> {
        if self.entries.get(key).is_some_and(|entry| self.ttl.is_some_and(|ttl| now.saturating_duration_since(entry.written) >= ttl)) {
            self.entries.remove(key); self.order.retain(|entry| entry != key);
        }
        let value = Arc::clone(&self.entries.get(key)?.value);
        self.order.retain(|entry| entry != key); self.order.push_back(key.clone());
        Some(value)
    }
    pub fn insert(&mut self, key: Key, value: Arc<T>, now: Instant) {
        if self.capacity == 0 || self.ttl == Some(Duration::ZERO) { return; }
        self.entries.remove(&key); self.order.retain(|entry| entry != &key);
        while self.entries.len() >= self.capacity {
            if let Some(key) = self.order.pop_front() { self.entries.remove(&key); }
        }
        self.order.push_back(key.clone()); self.entries.insert(key, Entry { value, written: now });
    }
}

fn overflow() -> rtdb_types::Anomaly { error("allocation-overflow", "Reader schema request exceeds addressable memory") }

fn metadata(text: &str) -> Result<BTreeMap<String, String>> {
    let mut values = BTreeMap::new();
    if text.is_empty() { return Ok(values); }
    let mut characters = text.chars().peekable();
    loop {
        let mut field = String::new();
        let mut ended = false;
        if characters.peek() == Some(&'\'') {
            characters.next();
            loop {
                let character = characters.next().ok_or_else(|| error("invalid-plugin-config", "Metadata selector is missing its closing quote"))?;
                if character != '\'' { field.push(character); continue; }
                if characters.peek() == Some(&'\'') { characters.next(); field.push('\''); continue; }
                break;
            }
            loop {
                match characters.next() {
                    Some(',') => break,
                    None | Some('\r' | '\n') => { ended = true; break; }
                    Some(character) if character <= ' ' => (),
                    Some(_) => return Err(error("invalid-plugin-config", "Metadata selector requires a comma after a quoted field")),
                }
            }
        } else {
            loop {
                match characters.next() {
                    Some(',') => break,
                    None | Some('\r' | '\n') => { ended = true; break; }
                    Some(character) => field.push(character),
                }
            }
        }
        let (key, value) = field.split_once('=').ok_or_else(|| error("invalid-plugin-config", "Metadata selector entries require key=value"))?;
        if values.insert(key.into(), value.into()).is_some() { return Err(error("invalid-plugin-config", "Metadata selector contains a duplicate key")); }
        if ended { return Ok(values); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metadata_selector_preserves_csv_quotes_spaces_and_first_record() {
        let values = metadata("'key=one,two','quote=a''b', spaced = value ,empty=,=value\nignored=second").unwrap();
        assert_eq!(values, BTreeMap::from([("key".into(), "one,two".into()), ("quote".into(), "a'b".into()), (" spaced ".into(), " value ".into()), ("empty".into(), "".into()), ("".into(), "value".into())]));
        assert_eq!(metadata("'key=line\nbreak' \t,tail=x").unwrap()["key"], "line\nbreak");
        assert!(metadata("x=1,x=2").is_err());
        assert!(metadata("'x=1'y").is_err());
        assert!(metadata("x=1,").is_err());
        assert!(metadata("'x=1").is_err());
    }

    #[test]
    fn empty_metadata_suppresses_latest_version_and_cache_keys_follow_selection() {
        let options = BTreeMap::from([("use.latest.version".into(), "true".into()), ("use.latest.with.metadata".into(), "".into())]);
        assert!(matches!(Selection::parse(&options).unwrap(), Selection::Writer));
        assert!(Selection::Latest.key("topic-value", 1) != Selection::Latest.key("topic-value", 2));
        let metadata = Selection::Metadata(BTreeMap::from([("tag".into(), "v1".into())]));
        assert!(metadata.key("topic-value", 1) == metadata.key("topic-value", 2));
        assert_eq!(metadata.request("a/b").unwrap(), (vec!["subjects".into(), "a/b".into(), "metadata".into()], vec![("key".into(), "tag".into()), ("value".into(), "v1".into()), ("deleted".into(), "true".into())]));
    }

    #[test]
    fn latest_cache_expiry_is_since_write_and_access_updates_eviction_order() {
        let mut cache = LatestCache::open(&BTreeMap::from([("latest.cache.size".into(), "2".into()), ("latest.cache.ttl.sec".into(), "10".into())])).unwrap();
        let key = |id| Key::Latest { subject: "topic-value".into(), writer: id };
        let start = Instant::now();
        cache.insert(key(1), Arc::new(1), start); cache.insert(key(2), Arc::new(2), start);
        assert_eq!(*cache.get(&key(1), start + Duration::from_secs(9)).unwrap(), 1);
        cache.insert(key(3), Arc::new(3), start + Duration::from_secs(9));
        assert!(cache.get(&key(2), start + Duration::from_secs(9)).is_none());
        assert!(cache.get(&key(1), start + Duration::from_secs(10)).is_none());
        assert_eq!(*cache.get(&key(3), start + Duration::from_secs(10)).unwrap(), 3);
    }

    #[test]
    fn zero_capacity_and_zero_ttl_do_not_retain_latest_entries() {
        for option in ["latest.cache.size", "latest.cache.ttl.sec"] {
            let mut cache = LatestCache::open(&BTreeMap::from([(option.into(), "0".into())])).unwrap();
            let key = Key::Metadata("topic-value".into()); let value = Arc::new(1); let now = Instant::now();
            cache.insert(key.clone(), Arc::clone(&value), now);
            assert_eq!(Arc::strong_count(&value), 1); assert!(cache.get(&key, now).is_none());
        }
    }
}
