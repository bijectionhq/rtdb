use rtdb_types::{Result, java_text::JavaText};
use crate::{EvalContext, numeric::overflow};

pub(super) struct PatternInput<'a> { pub original: JavaText<'a>, pub points: Vec<u32> }

pub(super) fn work_bytes(source: JavaText<'_>) -> Result<usize> {
    source.code_points().try_fold(0usize, |bytes, point| {
        bytes.checked_add(match point { 0..=0x7f => 1, 0x80..=0x7ff => 2, 0x800..=0xffff => 3, _ => 4 }).ok_or_else(overflow)
    })
}

pub(super) fn supplementary(point: u32) -> bool { point >= 0x10000 || (0xd800..=0xdfff).contains(&point) }

impl<'a> PatternInput<'a> {
    pub(super) fn new(original: JavaText<'a>, context: &EvalContext<'_>) -> Result<Self> {
        let count = original.code_points().count();
        let quoted_capacity = count.checked_mul(4).and_then(|count| count.checked_add(2)).ok_or_else(overflow)?;
        let bytes = count.checked_add(quoted_capacity).and_then(|count| count.checked_mul(std::mem::size_of::<u32>())).ok_or_else(overflow)?;
        context.reserve(bytes)?;
        let mut points = Vec::with_capacity(count);
        points.extend(original.code_points());
        let points = remove_quotes(&points, quoted_capacity);
        Ok(Self { original, points })
    }

    pub(super) fn text(&self, range: std::ops::Range<usize>) -> Option<String> {
        self.points[range].iter().copied().map(char::from_u32).collect()
    }
}

// OpenJDK 25 Pattern.RemoveQEQuoting operates on code points before parsing.
fn remove_quotes(points: &[u32], capacity: usize) -> Vec<u32> {
    let mut output = Vec::with_capacity(capacity);
    let mut points = points.iter().copied().peekable();
    let mut quoted = false;
    let mut first = false;
    while let Some(point) = points.next() {
        if !quoted {
            if point == u32::from(b'\\') && points.peek() == Some(&u32::from(b'Q')) {
                points.next(); quoted = true; first = true; continue;
            }
            output.push(point);
            if point == u32::from(b'\\') { if let Some(next) = points.next() { output.push(next); } }
        } else if point == u32::from(b'\\') && points.peek() == Some(&u32::from(b'E')) {
            points.next(); quoted = false;
        } else if point > 0x7f || (point as u8).is_ascii_alphabetic() {
            output.push(point);
        } else if (point as u8).is_ascii_digit() {
            if first { output.extend([u32::from(b'\\'), u32::from(b'x'), u32::from(b'3')]); }
            output.push(point);
        } else {
            output.push(u32::from(b'\\'));
            output.push(point);
        }
        first = false;
    }
    output
}
