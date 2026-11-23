use geometry::{GeometryWork, Gv};
use rtdb_types::Result;

use crate::kernel::{geometry_error, invalid};

pub(super) fn decode(mut bytes: &[u8], work: &mut GeometryWork) -> Result<Gv> {
    let value = read_value(&mut bytes, 0, work)?;
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

fn text(bytes: &mut &[u8], work: &mut GeometryWork) -> Result<String> {
    let mut decoded = Vec::new();
    loop {
        let end = bytes
            .iter()
            .position(|&byte| byte == 0)
            .ok_or_else(truncated)?;
        work.charge(end + 2).map_err(geometry_error)?;
        work.allocate(end + 1).map_err(geometry_error)?;
        decoded.extend_from_slice(take(bytes, end)?);
        match take(bytes, 2)? {
            [0, 1] => break,
            [0, 0xff] => decoded.push(0),
            _ => return Err(invalid("Invalid Bijection geometry text delimiter")),
        }
    }
    String::from_utf8(decoded).map_err(|_| invalid("Bijection geometry text must be UTF-8"))
}

fn read_value(bytes: &mut &[u8], depth: usize, work: &mut GeometryWork) -> Result<Gv> {
    if depth > 64 {
        return Err(invalid("Bijection geometry is too deeply nested"));
    }
    work.charge(1).map_err(geometry_error)?;
    work.allocate(std::mem::size_of::<Gv>())
        .map_err(geometry_error)?;
    Ok(match take(bytes, 1)?[0] {
        2 => Gv::Null,
        3 => match take(bytes, 1)?[0] {
            0 => Gv::Bool(false),
            1 => Gv::Bool(true),
            _ => return Err(invalid("Invalid Bijection geometry boolean")),
        },
        4 => {
            take(bytes, 8)?;
            Gv::Other
        }
        5 => {
            let encoded = u64::from_be_bytes(take(bytes, 8)?.try_into().expect("eight bytes"));
            let bits = if encoded < (1 << 63) {
                !encoded
            } else {
                encoded ^ (1 << 63)
            };
            let value = f64::from_bits(bits);
            if value.is_nan() && bits != 0x7ff8_0000_0000_0000 {
                return Err(invalid("Invalid Bijection geometry float"));
            }
            Gv::Float(value)
        }
        9 => Gv::String(text(bytes, work)?),
        12 => {
            let mut values = Vec::new();
            while !bytes.starts_with(&[0, 0]) {
                values.push(read_value(bytes, depth + 1, work)?);
            }
            take(bytes, 2)?;
            Gv::Array(values)
        }
        13 => {
            let mut fields: Vec<(String, Gv)> = Vec::new();
            while !bytes.starts_with(&[0, 0]) {
                let key = text(bytes, work)?;
                if fields.last().is_some_and(|(previous, _)| previous >= &key) {
                    return Err(invalid(
                        "Bijection geometry fields must be strictly ordered",
                    ));
                }
                work.allocate(std::mem::size_of::<String>())
                    .map_err(geometry_error)?;
                fields.push((key, read_value(bytes, depth + 1, work)?));
            }
            take(bytes, 2)?;
            Gv::Object(fields)
        }
        _ => return Err(invalid("Unsupported canonical Bijection geometry value")),
    })
}
