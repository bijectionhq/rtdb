use unicode_normalization::char::{canonical_combining_class, compose, decompose_canonical};

const MAX_DECOMPOSITION: usize = 4;

fn single_nfc(text: &str) -> Option<char> {
    let mut decomposed = ['\0'; MAX_DECOMPOSITION * MAX_DECOMPOSITION];
    let mut length = 0usize;
    for character in text.chars() {
        decompose_canonical(character, |value| {
            if let Some(slot) = decomposed.get_mut(length) { *slot = value; }
            length += 1;
        });
    }
    if length == 0 || length > decomposed.len() { return None; }
    for index in 1..length {
        let class = canonical_combining_class(decomposed[index]);
        if class != 0 {
            let mut position = index;
            while position > 0 && canonical_combining_class(decomposed[position - 1]) > class {
                decomposed.swap(position, position - 1);
                position -= 1;
            }
        }
    }
    decomposed[1..length].iter().try_fold(decomposed[0], |value, next| compose(value, *next))
}

pub(crate) fn candidates(text: &str, start: usize, mut accepts: impl FnMut(char) -> bool) -> [Option<usize>; MAX_DECOMPOSITION] {
    let mut result = [None; MAX_DECOMPOSITION];
    let Some(cluster_end) = crate::grapheme::next_boundary(text, start) else { return result; };
    let mut ends = [0; MAX_DECOMPOSITION];
    let mut count = 0;
    for (offset, character) in text[start..cluster_end].char_indices().take(MAX_DECOMPOSITION) {
        ends[count] = start + offset + character.len_utf8();
        count += 1;
    }
    if ends[0] == cluster_end {
        if accepts(text[start..].chars().next().unwrap()) { result[0] = Some(cluster_end); }
    } else {
        let mut selected = 0;
        for index in (1..count).rev() {
            if single_nfc(&text[start..ends[index]]).is_some_and(&mut accepts) {
                result[selected] = Some(ends[index]);
                selected += 1;
            }
        }
    }
    result
}

pub(crate) fn candidates_utf16(text: &[u16], start: usize, mut accepts: impl FnMut(u32) -> bool) -> [Option<usize>; MAX_DECOMPOSITION] {
    let mut result = [None; MAX_DECOMPOSITION];
    let Some(cluster_end) = crate::grapheme::next_boundary_utf16(text, start) else { return result; };
    let Some((first, first_end)) = crate::java::point(text, start) else { return result; };
    if first_end == cluster_end { if accepts(first) { result[0] = Some(first_end); } return result; }
    let mut bytes = [0; MAX_DECOMPOSITION * 4];
    let mut ends = [(0, 0); MAX_DECOMPOSITION];
    let mut count = 0; let mut length = 0; let mut position = start;
    while position < cluster_end && count < MAX_DECOMPOSITION {
        let Some((value, end)) = crate::java::point(text, position) else { break; };
        let Some(character) = char::from_u32(value) else { return result; };
        length += character.encode_utf8(&mut bytes[length..]).len();
        ends[count] = (length, end); count += 1; position = end;
    }
    let mut selected = 0;
    for &(length, end) in ends[1..count].iter().rev() {
        if core::str::from_utf8(&bytes[..length]).ok().and_then(single_nfc).is_some_and(|character| accepts(character as u32)) {
            result[selected] = Some(end); selected += 1;
        }
    }
    result
}
