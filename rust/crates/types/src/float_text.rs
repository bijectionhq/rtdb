/// Uses fixed notation for decimal exponents -3 through 6 and scientific notation elsewhere.
/// Preserves signed zero and uses `NaN`, `Infinity`, and `-Infinity` for non-finite values.
pub fn java_f64_text(value: f64) -> String { format_float(value, None) }

/// Formats a single-precision value without widening its shortest decimal spelling.
pub fn java_f32_text(value: f32) -> String { format_float(f64::from(value), Some(value)) }

fn format_float(value: f64, single: Option<f32>) -> String {
    if value.is_nan() { return "NaN".into(); }
    if value == f64::INFINITY { return "Infinity".into(); }
    if value == f64::NEG_INFINITY { return "-Infinity".into(); }
    if value == 0.0 { return if value.is_sign_negative() { "-0.0" } else { "0.0" }.into(); }
    let mut text = single.map_or_else(|| format!("{value:?}"), |value| format!("{value:?}"));
    let negative = text.starts_with('-');
    if negative { text.remove(0); }
    let (mantissa, power) = text.split_once('e').map_or((text.as_str(), 0), |(mantissa, power)| (mantissa, power.parse::<i32>().unwrap_or(0)));
    let dot = mantissa.find('.').unwrap_or(mantissa.len());
    let mut digits = mantissa.replace('.', "");
    let leading = digits.bytes().take_while(|byte| *byte == b'0').count();
    let mut exponent = dot as i32 - leading as i32 - 1 + power;
    digits.drain(..leading);
    while digits.len() > 1 && digits.ends_with('0') { digits.pop(); }
    let mut result = if negative { "-".to_owned() } else { String::new() };
    if (-3..7).contains(&exponent) {
        let place = exponent + 1;
        if place <= 0 { result.push_str("0."); result.extend(std::iter::repeat_n('0', (-place) as usize)); result.push_str(&digits); }
        else if place as usize >= digits.len() { result.push_str(&digits); result.extend(std::iter::repeat_n('0', place as usize - digits.len())); result.push_str(".0"); }
        else { result.push_str(&digits[..place as usize]); result.push('.'); result.push_str(&digits[place as usize..]); }
    } else {
        if digits.len() == 1 {
            let precise = single.map_or_else(|| format!("{:.1e}", value.abs()), |value| format!("{:.1e}", value.abs()));
            let (mantissa, power) = precise.split_once('e').expect("scientific format contains exponent");
            digits = mantissa.replace('.', ""); exponent = power.parse().expect("scientific exponent is an integer");
        }
        result.push_str(&digits[..1]); result.push('.'); result.push_str(&digits[1..]); result.push('E'); result.push_str(&exponent.to_string());
    }
    result
}
