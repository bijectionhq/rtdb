use std::{fmt::Write as _, io::Write as _};

/// Uses fixed notation for decimal exponents -3 through 6 and scientific notation elsewhere.
/// Preserves signed zero and uses `NaN`, `Infinity`, and `-Infinity` for non-finite values.
pub fn java_f64_text(value: f64) -> String {
    let mut text = String::new();
    write_java_float(&mut text, value, None);
    text
}

/// Formats a single-precision value without widening its shortest decimal spelling.
pub fn java_f32_text(value: f32) -> String {
    let mut text = String::new();
    write_java_float(&mut text, f64::from(value), Some(value));
    text
}

pub fn write_java_float(out: &mut String, value: f64, single: Option<f32>) {
    if value.is_sign_negative() && !value.is_nan() { out.push('-'); }
    let special = if value.is_nan() { Some("NaN") }
        else if value.is_infinite() { Some("Infinity") }
        else if value == 0.0 { Some("0.0") }
        else { None };
    if let Some(text) = special { out.push_str(text); return; }

    let mut buffer = [0; 64];
    let mut dst = buffer.as_mut_slice();
    match single {
        Some(value) => write!(dst, "{:?}", value.abs()),
        None => write!(dst, "{:?}", value.abs()),
    }.expect("float debug text fits");
    let length = 64 - dst.len();
    let text = str::from_utf8(&buffer[..length]).expect("float debug text is ASCII");
    let (mantissa, power) = text.split_once('e').map_or((text, 0), |(mantissa, power)| (mantissa, power.parse::<i32>().unwrap_or(0)));
    let dot = mantissa.find('.').unwrap_or(mantissa.len());
    let mut digits = [0; 64];
    let mut length = 0;
    let mut leading = 0;
    for byte in mantissa.bytes().filter(|&byte| byte != b'.') {
        if length == 0 && byte == b'0' { leading += 1; continue; }
        digits[length] = byte;
        length += 1;
    }
    while length > 1 && digits[length - 1] == b'0' { length -= 1; }
    let digits = str::from_utf8(&digits[..length]).expect("float digits are ASCII");
    let exponent = dot as i32 - leading - 1 + power;

    if (-3..7).contains(&exponent) {
        let place = exponent + 1;
        if place <= 0 { out.push_str("0."); out.extend(std::iter::repeat_n('0', (-place) as usize)); out.push_str(digits); }
        else if place as usize >= digits.len() { out.push_str(digits); out.extend(std::iter::repeat_n('0', place as usize - digits.len())); out.push_str(".0"); }
        else { out.push_str(&digits[..place as usize]); out.push('.'); out.push_str(&digits[place as usize..]); }
    } else if digits.len() == 1 {
        let mut dst = buffer.as_mut_slice();
        match single {
            Some(value) => write!(dst, "{:.1e}", value.abs()),
            None => write!(dst, "{:.1e}", value.abs()),
        }.expect("rounded float text fits");
        let length = 64 - dst.len();
        let precise = str::from_utf8(&buffer[..length]).expect("rounded float text is ASCII");
        let (mantissa, power) = precise.split_once('e').expect("scientific format contains exponent");
        let power: i32 = power.parse().expect("scientific exponent is an integer");
        write!(out, "{mantissa}E{power}").expect("writing to String");
    } else {
        write!(out, "{}.{}E{exponent}", &digits[..1], &digits[1..]).expect("writing to String");
    }
}
