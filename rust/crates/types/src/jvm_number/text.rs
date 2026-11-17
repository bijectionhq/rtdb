use num_bigint::{BigInt,Sign};

/// Formats a Java BigDecimal coefficient and signed scale without exponent-sized padding.
/// The caller admits coefficient scratch and the returned text; storage is proportional to coefficient digits.
pub fn decimal_parts_text(coefficient: &BigInt,scale: i32) -> String {
    let signed = coefficient.to_str_radix(10);
    let digits = signed.trim_start_matches('-');
    let adjusted = digits.len() as i64 - 1 - i64::from(scale);
    if scale == 0 { return signed; }
    let mut result = String::new();
    if coefficient.sign() == Sign::Minus { result.push('-'); }
    if scale >= 0 && adjusted >= -6 {
        let point = digits.len() as i64 - i64::from(scale);
        if point > 0 {
            let point = point as usize;
            result.push_str(&digits[..point]); result.push('.'); result.push_str(&digits[point..]);
        } else {
            result.push_str("0."); result.extend(std::iter::repeat_n('0', (-point) as usize)); result.push_str(digits);
        }
    } else {
        result.push_str(&digits[..1]);
        if digits.len() > 1 { result.push('.'); result.push_str(&digits[1..]); }
        result.push('E');
        if adjusted >= 0 { result.push('+'); }
        result.push_str(&adjusted.to_string());
    }
    result
}
