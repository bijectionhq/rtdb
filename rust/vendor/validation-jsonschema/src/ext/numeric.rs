use num_bigint::{BigInt, BigUint, Sign};
use num_traits::{ToPrimitive, Zero};
use serde_json::Number;
use std::cmp::Ordering;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub(crate) struct Numeric {
    negative: bool,
    digits: String,
    exponent: BigInt,
}
impl Numeric {
    pub(crate) fn new(number: &Number) -> Self {
        let text = number.as_str();
        let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
        let fraction = mantissa.split_once('.').map_or(0, |(_, value)| value.len());
        let mut exponent = exponent.parse::<BigInt>().expect("JSON number exponent") - BigInt::from(fraction);
        let digits: String = mantissa.chars().filter(char::is_ascii_digit).collect();
        let digits = digits.trim_start_matches('0');
        if digits.is_empty() { return Self { negative: false, digits: "0".into(), exponent: BigInt::zero() }; }
        let normalized = digits.trim_end_matches('0');
        exponent += BigInt::from(digits.len() - normalized.len());
        Self { negative: text.starts_with('-'), digits: normalized.into(), exponent }
    }
    pub(crate) fn is_integer(&self) -> bool { self.exponent.sign() != Sign::Minus }
    pub(crate) fn unsigned_limit(&self) -> Option<u64> {
        if self.negative || !self.is_integer() { return None; }
        if self.exponent.clone() + BigInt::from(self.digits.len()) > BigInt::from(20u8) { return Some(u64::MAX); }
        let mut value = self.digits.parse::<u64>().unwrap_or(u64::MAX);
        for _ in 0..self.exponent.to_usize().expect("bounded nonnegative exponent") { value = value.saturating_mul(10); }
        Some(value)
    }
    pub(crate) fn multiple_of(&self, divisor: &Self) -> bool {
        if divisor.digits == "0" { return false; }
        if self.digits == "0" { return true; }
        let value = BigUint::parse_bytes(self.digits.as_bytes(), 10).expect("decimal coefficient");
        let divisor_value = BigUint::parse_bytes(divisor.digits.as_bytes(), 10).expect("decimal coefficient");
        let exponent = &self.exponent - &divisor.exponent;
        if exponent.sign() != Sign::Minus {
            let exponent = exponent.to_biguint().expect("nonnegative exponent");
            let factor = BigUint::from(10u8).modpow(&exponent, &divisor_value);
            (value % &divisor_value * factor % divisor_value).is_zero()
        } else {
            if &value % &divisor_value != BigUint::zero() { return false; }
            let quotient = (value / divisor_value).to_str_radix(10);
            let zeros = quotient.len() - quotient.trim_end_matches('0').len();
            exponent.magnitude() <= &BigUint::from(zeros)
        }
    }
    fn magnitude_cmp(&self, other: &Self) -> Ordering {
        if self.digits == "0" { return if other.digits == "0" { Ordering::Equal } else { Ordering::Less }; }
        if other.digits == "0" { return Ordering::Greater; }
        let left = &self.exponent + BigInt::from(self.digits.len());
        let right = &other.exponent + BigInt::from(other.digits.len());
        left.cmp(&right).then_with(|| {
            let length = self.digits.len().max(other.digits.len());
            self.digits.bytes().chain(std::iter::repeat(b'0')).take(length)
                .cmp(other.digits.bytes().chain(std::iter::repeat(b'0')).take(length))
        })
    }
}
impl Ord for Numeric {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.negative, other.negative) {
            (true, false) => Ordering::Less,
            (false, true) => Ordering::Greater,
            (false, false) => self.magnitude_cmp(other),
            (true, true) => self.magnitude_cmp(other).reverse(),
        }
    }
}
impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> { Some(self.cmp(other)) }
}
pub(crate) fn ge(value: &Number, limit: &Numeric) -> bool { Numeric::new(value) >= *limit }
pub(crate) fn le(value: &Number, limit: &Numeric) -> bool { Numeric::new(value) <= *limit }
pub(crate) fn gt(value: &Number, limit: &Numeric) -> bool { Numeric::new(value) > *limit }
pub(crate) fn lt(value: &Number, limit: &Numeric) -> bool { Numeric::new(value) < *limit }
pub(crate) fn equal(left: &Number, right: &Number) -> bool { Numeric::new(left) == Numeric::new(right) }
pub(crate) fn is_integer(value: &Number) -> bool { Numeric::new(value).is_integer() }
pub(crate) fn is_literal_integer(value: &Number) -> bool { !value.as_str().contains(['.', 'e', 'E']) }
pub(crate) fn unsigned_limit(value: &Number, fractional: bool) -> Option<u64> {
    if !fractional && !is_literal_integer(value) { return None; }
    Numeric::new(value).unsigned_limit()
}

#[cfg(test)]
mod tests {
    use super::*;
    fn number(text: &str) -> Numeric { Numeric::new(&text.parse().unwrap()) }
    #[test]
    fn adjacent_big_integers_and_decimal_values_remain_exact() {
        assert!(number("18446744073709551616") < number("18446744073709551617"));
        assert!(number("-18446744073709551617") < number("-18446744073709551616"));
        assert_eq!(number("1.00"), number("1e0"));
        assert_eq!(number("-0.0"), number("0"));
        assert!(number("1e10000000000000000000000000") > number("9e9999999999999999999999999"));
        assert!(number("1e-10000000000000000000000000") < number("9e-9999999999999999999999999"));
    }
    #[test]
    fn divisibility_uses_coefficients_and_modular_exponents() {
        assert!(number("-18446744073709551618").multiple_of(&number("3")));
        assert!(!number("18446744073709551617").multiple_of(&number("3")));
        assert!(number("0.3").multiple_of(&number("0.1")));
        assert!(!number("0.3").multiple_of(&number("0.2")));
        assert!(number("1e10000000000000000000000000").multiple_of(&number("2")));
        assert!(!number("1e10000000000000000000000000").multiple_of(&number("3")));
        assert!(!number("1e-10000000000000000000000000").multiple_of(&number("2")));
        assert!(number("1").multiple_of(&number("0.25")));
        assert!(!number("1").multiple_of(&number("2.5")));
        assert!(number("0").multiple_of(&number("3")));
    }
}
