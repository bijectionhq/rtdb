//! Accounted exact arithmetic shared by topology and numerical constructions.
pub(crate) use num_rational::BigRational as Rational;

use crate::{
    GeometryRefusal,
    GeometryWork,
};

pub(crate) fn bytes(value: &Rational) -> usize {
    128usize.saturating_add(
        (value.numer().bits().saturating_add(value.denom().bits()) as usize)
            .div_ceil(8)
            .saturating_mul(4),
    )
}
/// Reserve integer arithmetic before it allocates. Scratch and result bounds
/// scale with operand precision; work exhaustion never changes topology.
pub(crate) fn arithmetic<T>(
    work: &mut GeometryWork,
    values: &[&Rational],
    calculate: impl FnOnce() -> T,
) -> Result<T, GeometryRefusal> {
    let bits = values
        .iter()
        .map(|value| value.numer().bits().max(value.denom().bits()))
        .max()
        .unwrap_or(1) as usize;
    let words = bits.max(1).div_ceil(64);
    work.scoped(|work| {
        work.charge(words.saturating_mul(words).saturating_mul(32))?;
        work.allocate(words.saturating_mul(2048).saturating_add(1024))?;
        Ok(calculate())
    })
}
pub(crate) fn binary64<const N: usize>(
    raw: [f64; N],
    work: &mut GeometryWork,
) -> Result<[Rational; N], GeometryRefusal> {
    work.charge(N)?;
    let retained = raw
        .iter()
        .map(|value| {
            let bits = value.to_bits();
            let exponent = ((bits >> 52) & 2047) as i32;
            let mantissa =
                (bits & ((1_u64 << 52) - 1)) | if exponent == 0 { 0 } else { 1_u64 << 52 };
            if mantissa == 0 {
                return 128;
            }
            let trailing = mantissa.trailing_zeros() as i32;
            let shift = if exponent == 0 {
                -1074
            } else {
                exponent - 1075
            } + trailing;
            let numerator = (64 - mantissa.leading_zeros()) as i32 - trailing + shift.max(0);
            let denominator = 1 + (-shift).max(0);
            128 + ((numerator + denominator) as usize)
                .div_ceil(8)
                .saturating_mul(4)
        })
        .sum::<usize>();
    work.allocate(retained)?;
    let value = raw.map(|value| Rational::from_float(value).expect("admitted finite coordinate"));
    Ok(value)
}
