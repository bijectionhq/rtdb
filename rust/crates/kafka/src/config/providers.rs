use std::{
    borrow::Cow,
    collections::{BTreeMap, BTreeSet},
    fs::{self, File},
    io::Read,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use rtdb_types::{Anomaly, MemoryBudget, Result};

mod file_text;
mod environment;
use environment::Environment;

type Properties = BTreeMap<String, String>;
type Values<'a> = BTreeMap<&'a str, String>;

const PROVIDERS: &str = "config.providers";
const FILE: &str = "org.apache.kafka.common.config.provider.FileConfigProvider";
const DIRECTORY: &str = "org.apache.kafka.common.config.provider.DirectoryConfigProvider";
const ENVIRONMENT: &str = "org.apache.kafka.common.config.provider.EnvVarConfigProvider";

pub(crate) fn configured(properties: &Properties) -> bool {
    properties.get(PROVIDERS).is_some_and(|aliases| !aliases.is_empty())
}

pub(crate) fn control_property(name: &str) -> bool {
    name == PROVIDERS || name.starts_with("config.providers.")
}

pub(crate) fn resolve<'a>(
    original: &'a Properties, memory: &Arc<MemoryBudget>, reserve: &mut impl FnMut(usize) -> Result<()>,
) -> Result<Cow<'a, Properties>> {
    let Some(aliases) = original.get(PROVIDERS).filter(|aliases| !aliases.is_empty()) else {
        return Ok(Cow::Borrowed(original));
    };
    let text_bytes = original.iter().try_fold(0usize, |sum, (name, value)| add(add(sum, name.len())?, value.len()))?;
    reserve(add(product(text_bytes, 64)?, add(product(original.len(), 512)?, 4096)?)?)?;
    let mut providers = BTreeMap::new();
    for alias in list(aliases) {
        if providers.contains_key(alias) { continue; }
        let prefix = format!("{PROVIDERS}.{alias}.");
        if let Some(class) = original.get(&format!("{prefix}class")) {
            let allowed = original.get(&format!("{prefix}param.allowed.paths")).map(String::as_str);
            let provider = match class.as_str() {
                FILE => Provider::File(AllowedPaths::new(allowed, reserve)?),
                DIRECTORY => Provider::Directory(AllowedPaths::new(allowed, reserve)?),
                ENVIRONMENT => {
                    let pattern = original.get(&format!("{prefix}param.allowlist.pattern")).map(String::as_str);
                    Provider::Environment(Environment::capture(pattern, memory)?)
                }
                _ => return Err(Anomaly::unsupported("xtdb.kafka/config-provider-class", "Configured Kafka ConfigProvider class requires a native adapter")),
            };
            providers.insert(alias, provider);
        }
    }

    let mut requests = BTreeMap::<(&str, &str), BTreeSet<&str>>::new();
    for value in original.values() {
        for variable in Variables::new(value) {
            if providers.contains_key(variable.provider) {
                requests.entry((variable.provider, variable.path)).or_default().insert(variable.key);
            }
        }
    }
    if requests.is_empty() {
        return Ok(Cow::Borrowed(original));
    }
    let mut lookups = BTreeMap::new();
    for ((alias, path), keys) in requests {
        let values = providers[alias].get(path, &keys, reserve)?;
        lookups.insert((alias, path), values);
    }
    let replacement = |variable: &Variable<'a>| {
        lookups.get(&(variable.provider, variable.path))?.get(variable.key).map(String::as_str)
    };
    let mut output = BTreeMap::new();
    for (name, value) in original {
        let length = Variables::new(value).try_fold(value.len(), |length, variable| {
            match replacement(&variable) {
                Some(text) => add(length.checked_sub(variable.end - variable.start).ok_or_else(allocation)?, text.len()),
                None => Ok(length),
            }
        })?;
        reserve(length)?;
        let mut resolved = String::new();
        resolved.try_reserve_exact(length).map_err(|_| allocation())?;
        let mut copied = 0;
        for variable in Variables::new(value) {
            if let Some(text) = replacement(&variable) {
                resolved.push_str(&value[copied..variable.start]);
                resolved.push_str(text);
                copied = variable.end;
            }
        }
        resolved.push_str(&value[copied..]);
        output.insert(name.clone(), resolved);
    }
    Ok(Cow::Owned(output))
}

fn list(value: &str) -> impl Iterator<Item = &str> {
    let items = value.trim_end_matches(',');
    items.split(',').take(if items.is_empty() { 0 } else { usize::MAX })
}

struct Variable<'a> { provider: &'a str, path: &'a str, key: &'a str, start: usize, end: usize }
struct Variables<'a> { input: &'a str, position: usize }
impl<'a> Variables<'a> {
    fn new(input: &'a str) -> Self { Self { input, position: 0 } }
}
impl<'a> Iterator for Variables<'a> {
    type Item = Variable<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(offset) = self.input[self.position..].find("${") {
            let start = self.position + offset;
            let contents = &self.input[start + 2..];
            let close = contents.find('}')?;
            self.position = start + 3 + close;
            if let Some((provider, rest)) = contents[..close].split_once(':') {
                let (path, key) = rest.split_once(':').unwrap_or(("", rest));
                return Some(Variable { provider, path, key, start, end: self.position });
            }
        }
        None
    }
}

struct AllowedPaths(Option<Vec<PathBuf>>);
impl AllowedPaths {
    fn new(config: Option<&str>, reserve: &mut impl FnMut(usize) -> Result<()>) -> Result<Self> {
        let Some(config) = config.filter(|text| !text.is_empty()) else { return Ok(Self(None)); };
        let mut roots = Vec::new();
        for root in list(config) {
            let path = input_path(root)?;
            if !path.is_absolute() { return Err(invalid("Kafka ConfigProvider root must be absolute")); }
            let normalized = normalize_absolute(path);
            let canonical = fs::canonicalize(normalized).map_err(|_| invalid("Kafka ConfigProvider root could not be resolved"))?;
            reserve(add(product(canonical.as_os_str().len(), 4)?, 256)?)?;
            roots.push(canonical);
        }
        Ok(Self(Some(roots)))
    }

    fn path<'a>(&self, input: &'a str) -> Result<Option<Cow<'a, Path>>> {
        let path = input_path(input)?;
        let Some(roots) = &self.0 else { return Ok(Some(Cow::Borrowed(path))); };
        let Ok(canonical) = fs::canonicalize(path) else { return Ok(None); };
        Ok(roots.iter().any(|root| canonical.starts_with(root)).then_some(Cow::Owned(canonical)))
    }
}

fn input_path(text: &str) -> Result<&Path> {
    if text.as_bytes().contains(&0) { return Err(invalid("Kafka ConfigProvider path contains a NUL unit")); }
    Ok(Path::new(text))
}

fn normalize_absolute(path: &Path) -> PathBuf {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::ParentDir => { normalized.pop(); }
            Component::CurDir => (),
            part => normalized.push(part.as_os_str()),
        }
    }
    normalized
}

enum Provider { File(AllowedPaths), Directory(AllowedPaths), Environment(Environment) }
impl Provider {
    fn get<'a>(&self, path: &str, keys: &BTreeSet<&'a str>, reserve: &mut impl FnMut(usize) -> Result<()>) -> Result<Values<'a>> {
        match self {
            Self::Environment(environment) => environment.get(path, keys, reserve),
            _ if path.is_empty() => Ok(BTreeMap::new()),
            Self::File(allowed) => {
                let Some(path) = allowed.path(path)? else { return Ok(BTreeMap::new()); };
                let source = read_text(&path, reserve)?;
                reserve(add(product(source.len(), 96)?, 4096)?)?;
                file_text::selected(&source, keys)
            }
            Self::Directory(allowed) => {
                let Some(path) = allowed.path(path)?.filter(|path| path.is_dir()) else { return Ok(BTreeMap::new()); };
                let entries = fs::read_dir(&path).map_err(|_| invalid("Kafka ConfigProvider directory could not be listed"))?;
                let mut values = BTreeMap::new();
                for entry in entries {
                    let entry = entry.map_err(|_| invalid("Kafka ConfigProvider directory entry could not be read"))?;
                    let name = entry.file_name();
                    if let Some(&key) = name.to_str().and_then(|name| keys.get(name)).filter(|_| entry.path().is_file()) {
                        values.insert(key, read_text(&entry.path(), reserve)?);
                    }
                }
                Ok(values)
            }
        }
    }
}

fn read_text(path: &Path, reserve: &mut impl FnMut(usize) -> Result<()>) -> Result<String> {
    let mut file = File::open(path).map_err(|_| invalid("Kafka ConfigProvider file could not be opened"))?;
    let mut bytes = Vec::new();
    let mut block = [0; 8192];
    loop {
        let length = match file.read(&mut block) {
            Ok(0) => break,
            Ok(length) => length,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return Err(invalid("Kafka ConfigProvider file could not be read")),
        };
        let needed = add(bytes.len(), length)?;
        if needed > bytes.capacity() {
            let capacity = product(bytes.capacity(), 2)?.max(needed);
            reserve(capacity)?;
            bytes.try_reserve_exact(capacity - bytes.len()).map_err(|_| allocation())?;
        }
        bytes.extend_from_slice(&block[..length]);
    }
    String::from_utf8(bytes).map_err(|_| invalid("Kafka ConfigProvider file is not valid UTF-8"))
}

fn add(left: usize, right: usize) -> Result<usize> { left.checked_add(right).ok_or_else(allocation) }
fn product(left: usize, right: usize) -> Result<usize> { left.checked_mul(right).ok_or_else(allocation) }
fn allocation() -> Anomaly { Anomaly::incorrect("xtdb.kafka/config-provider-allocation", "Kafka ConfigProvider allocation exceeds its admitted capacity") }
fn invalid(message: &'static str) -> Anomaly { Anomaly::incorrect("xtdb.kafka/config-provider", message) }

#[cfg(test)]
mod tests;
