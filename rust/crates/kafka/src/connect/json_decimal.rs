use rtdb_types::Result;
use super::error;

#[derive(Clone, Debug)]
pub(super) struct JsonDecimal {
    negative: bool,
    digits: Vec<u8>,
    scale: i32,
    float: f32,
    double: f64,
}
impl JsonDecimal {
    pub fn parse(text: &str) -> Result<Self> {
        let (mantissa, exponent) = text.split_once(['e', 'E']).unwrap_or((text, "0"));
        let exponent = exponent.parse::<i64>().map_err(|cause| error("conversion-error", cause))?;
        let fraction = mantissa.split_once('.').map_or(0, |(_, fraction)| fraction.len());
        let scale = i64::try_from(fraction).ok().and_then(|fraction| fraction.checked_sub(exponent)).and_then(|scale| i32::try_from(scale).ok()).ok_or_else(|| error("conversion-error", "JSON decimal scale exceeds Java BigDecimal range"))?;
        let digits: Vec<_> = mantissa.bytes().filter(u8::is_ascii_digit).map(|digit| digit - b'0')
            .skip_while(|digit| *digit == 0).collect();
        let zero = digits.is_empty();
        Ok(Self {
            negative: !zero && text.starts_with('-'),
            digits: if zero { vec![0] } else { digits },
            scale,
            float: if zero { 0.0 } else { text.parse().expect("validated JSON decimal") },
            double: if zero { 0.0 } else { text.parse().expect("validated JSON decimal") },
        })
    }
    pub fn long_value(&self) -> i64 {
        let end = if self.scale > 0 { self.digits.len().saturating_sub(self.scale as usize) } else { self.digits.len() };
        let mut value = self.digits[..end].iter().fold(0u64, |value, digit| value.wrapping_mul(10).wrapping_add(u64::from(*digit)));
        if self.scale < 0 {
            let exponent = -i64::from(self.scale);
            if exponent >= 64 { return 0; }
            for _ in 0..exponent { value = value.wrapping_mul(10); }
        }
        if self.negative { value.wrapping_neg() as i64 } else { value as i64 }
    }
    pub fn double_value(&self) -> f64 { self.double }
    pub fn float_value(&self) -> f32 { self.float }
    pub fn encoded_bound(&self) -> usize { self.digits.len().saturating_add(32) }
    pub fn text(&self) -> String {
        let mut text = String::with_capacity(self.encoded_bound());
        if self.negative { text.push('-'); }
        let adjusted = self.digits.len() as i64 - 1 - i64::from(self.scale);
        if self.scale >= 0 && adjusted >= -6 {
            let point = self.digits.len() as i64 - i64::from(self.scale);
            if point <= 0 {
                text.push_str("0.");
                for _ in 0..-point { text.push('0'); }
                push_digits(&mut text, &self.digits);
            } else {
                let point = point as usize;
                push_digits(&mut text, &self.digits[..point]);
                if point < self.digits.len() { text.push('.'); push_digits(&mut text, &self.digits[point..]); }
            }
        } else {
            push_digits(&mut text, &self.digits[..1]);
            if self.digits.len() > 1 { text.push('.'); push_digits(&mut text, &self.digits[1..]); }
            text.push('E');
            if adjusted >= 0 { text.push('+'); }
            text.push_str(&adjusted.to_string());
        }
        text
    }
}
impl PartialEq for JsonDecimal {
    fn eq(&self, other: &Self) -> bool {
        if self.digits == [0] && other.digits == [0] { return true; }
        let left = trim_zeroes(&self.digits);
        let right = trim_zeroes(&other.digits);
        self.negative == other.negative && left == right
            && i64::from(self.scale) - (self.digits.len() - left.len()) as i64 == i64::from(other.scale) - (other.digits.len() - right.len()) as i64
    }
}

fn push_digits(text: &mut String, digits: &[u8]) {
    text.extend(digits.iter().map(|digit| char::from(b'0' + *digit)));
}

fn trim_zeroes(digits: &[u8]) -> &[u8] {
    &digits[..digits.iter().rposition(|digit| *digit != 0).map_or(0, |end| end + 1)]
}
