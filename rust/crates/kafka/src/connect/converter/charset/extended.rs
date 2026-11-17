mod names;

pub(super) fn lookup(name: &str) -> Option<&'static str> {
    let index = names::NAMES.binary_search_by(|&(key, _)| {
        key.bytes().map(u32::from).cmp(name.chars().map(fold))
    }).ok()?;
    Some(names::NAMES[index].1)
}

fn fold(unit: char) -> u32 {
    match unit {
        '\u{130}' | '\u{131}' => u32::from(b'i'),
        '\u{17f}' => u32::from(b's'),
        '\u{212a}' => u32::from(b'k'),
        unit => unit.to_ascii_lowercase() as u32,
    }
}

#[cfg(test)]
mod tests;
