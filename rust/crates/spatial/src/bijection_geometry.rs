use std::borrow::Cow;

use geometry::{GeometryWork, Gv, Scalar, ValueView};
use rtdb_types::Result;

use crate::kernel::{geometry_error, invalid};

#[derive(Clone, Copy)]
pub(super) struct Value<'a>(&'a [u8]);

pub(super) fn decode<'a>(mut bytes: &'a [u8], work: &mut GeometryWork) -> Result<Value<'a>> {
    let value = Value(bytes);
    validate(&mut bytes, 0, work)?;
    if !bytes.is_empty() {
        return Err(invalid("Bijection geometry has trailing bytes"));
    }
    Ok(value)
}

fn take<'a>(bytes: &mut &'a [u8], length: usize) -> Result<&'a [u8]> {
    let (value, tail) = (*bytes).split_at_checked(length).ok_or_else(truncated)?;
    *bytes = tail;
    Ok(value)
}

fn truncated() -> rtdb_types::Anomaly {
    invalid("Bijection geometry is truncated")
}

fn text<'a>(bytes: &mut &'a [u8], mut work: Option<&mut GeometryWork>) -> Result<Cow<'a, str>> {
    let mut escaped = None::<Vec<u8>>;
    loop {
        let end = bytes.iter().position(|&byte| byte == 0).ok_or_else(truncated)?;
        if let Some(work) = work.as_deref_mut() {
            work.charge(end + 2).map_err(geometry_error)?;
            work.allocate(end + 1).map_err(geometry_error)?;
        }
        let chunk = take(bytes, end)?;
        match take(bytes, 2)? {
            [0, 1] => {
                let invalid_utf8 = |_| invalid("Bijection geometry text must be UTF-8");
                return if let Some(mut decoded) = escaped {
                    decoded.extend_from_slice(chunk);
                    String::from_utf8(decoded).map(Cow::Owned)
                        .map_err(|_| invalid("Bijection geometry text must be UTF-8"))
                } else {
                    std::str::from_utf8(chunk).map(Cow::Borrowed).map_err(invalid_utf8)
                };
            }
            [0, 0xff] => {
                let decoded = escaped.get_or_insert_with(Vec::new);
                decoded.extend_from_slice(chunk);
                decoded.push(0);
            }
            _ => return Err(invalid("Invalid Bijection geometry text delimiter")),
        }
    }
}

fn float(bytes: &[u8]) -> f64 {
    let encoded = u64::from_be_bytes(bytes.try_into().expect("eight bytes"));
    f64::from_bits(if encoded < (1 << 63) { !encoded } else { encoded ^ (1 << 63) })
}

fn validate(bytes: &mut &[u8], depth: usize, work: &mut GeometryWork) -> Result<()> {
    if depth > 64 {
        return Err(invalid("Bijection geometry is too deeply nested"));
    }
    work.charge(1).map_err(geometry_error)?;
    // Preserve the admission allowance for a stored image regardless of its decoder.
    work.allocate(std::mem::size_of::<Gv>()).map_err(geometry_error)?;
    match take(bytes, 1)?[0] {
        2 => (),
        3 => {
            if take(bytes, 1)?[0] > 1 {
                return Err(invalid("Invalid Bijection geometry boolean"));
            }
        }
        4 => { take(bytes, 8)?; }
        5 => {
            let value = float(take(bytes, 8)?);
            if value.is_nan() && value.to_bits() != 0x7ff8_0000_0000_0000 {
                return Err(invalid("Invalid Bijection geometry float"));
            }
        }
        9 => { text(bytes, Some(work))?; }
        12 => {
            while !bytes.starts_with(&[0, 0]) {
                validate(bytes, depth + 1, work)?;
            }
            take(bytes, 2)?;
        }
        13 => {
            let mut previous = None;
            while !bytes.starts_with(&[0, 0]) {
                let key = text(bytes, Some(work))?;
                if previous.as_ref().is_some_and(|previous| previous >= &key) {
                    return Err(invalid("Bijection geometry fields must be strictly ordered"));
                }
                work.allocate(std::mem::size_of::<String>()).map_err(geometry_error)?;
                previous = Some(key);
                validate(bytes, depth + 1, work)?;
            }
            take(bytes, 2)?;
        }
        _ => return Err(invalid("Unsupported canonical Bijection geometry value")),
    }
    Ok(())
}

fn skip_text(bytes: &mut &[u8]) {
    loop {
        let end = bytes.iter().position(|&byte| byte == 0).expect("validated text");
        let last = bytes[end + 1] == 1;
        *bytes = &bytes[end + 2..];
        if last { return; }
    }
}

fn skip(bytes: &mut &[u8]) {
    let (&tag, tail) = bytes.split_first().expect("validated value");
    *bytes = tail;
    match tag {
        2 => (),
        3 => *bytes = &bytes[1..],
        4 | 5 => *bytes = &bytes[8..],
        9 => skip_text(bytes),
        12 | 13 => {
            while !bytes.starts_with(&[0, 0]) {
                if tag == 13 { skip_text(bytes); }
                skip(bytes);
            }
            *bytes = &bytes[2..];
        }
        _ => unreachable!("validated value tag"),
    }
}

impl<'a> ValueView<'a> for Value<'a> {
    fn scalar(self) -> Scalar {
        match self.0[0] {
            2 => Scalar::Null,
            3 => Scalar::Bool(self.0[1] == 1),
            5 => Scalar::Float(float(&self.0[1..9])),
            _ => Scalar::Other,
        }
    }

    fn text(self) -> Option<Cow<'a, str>> {
        let mut rest = self.0.strip_prefix(&[9])?;
        Some(text(&mut rest, None).expect("validated text"))
    }

    fn array(self) -> Option<impl ExactSizeIterator<Item = Self>> {
        let mut rest = self.0.strip_prefix(&[12])?;
        let mut end = rest;
        let mut len = 0;
        while !end.starts_with(&[0, 0]) {
            skip(&mut end);
            len += 1;
        }
        Some((0..len).map(move |_| {
            let value = Self(rest);
            skip(&mut rest);
            value
        }))
    }

    fn object(self) -> Option<impl Iterator<Item = (Cow<'a, str>, Self)>> {
        let mut rest = self.0.strip_prefix(&[13])?;
        Some(std::iter::from_fn(move || {
            if rest.starts_with(&[0, 0]) { return None; }
            let key = text(&mut rest, None).expect("validated key");
            let value = Self(rest);
            skip(&mut rest);
            Some((key, value))
        }))
    }
}
