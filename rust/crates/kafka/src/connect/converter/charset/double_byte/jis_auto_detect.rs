use super::{euc_jp::EucJp, iso2022jp::Iso2022Jp, probe::ProbeStop};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Selection { Iso2022Jp, EucJp, ShiftJis, Incomplete }

#[derive(Default)]
struct JapaneseCounts { hiragana: u8, katakana: u8 }

impl JapaneseCounts {
    fn observe(&mut self, unit: u16) {
        if (0x3040..=0x309f).contains(&unit) { self.hiragana = (self.hiragana + 1).min(2); }
        if (0xff65..=0xff9f).contains(&unit) { self.katakana = (self.katakana + 1).min(2); }
    }

    fn looks_japanese(&self) -> bool { self.hiragana == 2 || self.katakana == 2 }
}

fn select(bytes: &[u8]) -> Selection {
    if Iso2022Jp::Standard.probe(bytes).stop == ProbeStop::Underflow { return Selection::Iso2022Jp; }
    let mut japanese = JapaneseCounts::default();
    let euc = EucJp::Standard.probe(bytes, |unit| japanese.observe(unit));
    if euc.stop != ProbeStop::Underflow { return Selection::ShiftJis; }
    let sjis = super::lookup("shift_jis").expect("registered Shift_JIS mapping").probe(bytes);
    if sjis.stop != ProbeStop::Underflow { return Selection::EucJp; }
    match euc.consumed.cmp(&sjis.consumed) {
        std::cmp::Ordering::Greater => Selection::EucJp,
        std::cmp::Ordering::Less => Selection::ShiftJis,
        std::cmp::Ordering::Equal if euc.consumed == 0 => Selection::Incomplete,
        std::cmp::Ordering::Equal => if japanese.looks_japanese() { Selection::EucJp } else { Selection::ShiftJis },
    }
}

pub(in super::super) fn decode(bytes: &[u8]) -> String {
    let prefix = bytes.iter().take_while(|&&byte| byte < 0x80 && byte != 0x1b).count();
    let suffix = &bytes[prefix..];
    let selected = (!suffix.is_empty()).then(|| select(suffix));
    let mut output = String::with_capacity(bytes.len() * 3);
    output.extend(bytes[..prefix].iter().copied().map(char::from));
    match selected {
        None => {},
        Some(Selection::Iso2022Jp) => Iso2022Jp::Standard.decode_into(suffix, &mut output),
        Some(Selection::EucJp) => EucJp::Standard.decode_into(suffix, &mut output),
        Some(Selection::ShiftJis) => super::lookup("shift_jis").expect("registered Shift_JIS mapping").decode_into(suffix, &mut output),
        Some(Selection::Incomplete) => output.push('\u{fffd}'),
    }
    output
}

#[cfg(test)]
mod tests;
