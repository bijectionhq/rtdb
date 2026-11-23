use rand_core::RngCore;
use rtdb_types::{Anomaly, Result, Value};

pub(crate) fn random() -> Result<f64> {
    let mut bytes = [0u8; 8];
    rand_core::OsRng.try_fill_bytes(&mut bytes).map_err(|error| Anomaly::fault("rtdb.expr/random-source", error.to_string()))?;
    Ok((u64::from_ne_bytes(bytes) >> 11) as f64 * (1.0 / 9_007_199_254_740_992.0))
}

pub(crate) fn sleep_millis(value: &Value) -> Result<u64> {
    let Value::Duration { value, unit } = value else {
        return if matches!(value, Value::Null | Value::Undefined) { Ok(0) }
            else { Err(Anomaly::incorrect("xtdb.expression/function-type-mismatch", "sleep requires a duration")) };
    };
    // expression.clj/sleep multiplies under *unchecked-math*; this is not duration-unit conversion.
    let millis = value.wrapping_mul(unit.units_per_second() / 1000);
    u64::try_from(millis).map_err(|_| Anomaly::incorrect("xtdb/illegal-argument", "timeout value is negative"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rtdb_types::TimeUnit::*;

    #[test]
    fn expression_clj_sleep_preserves_its_literal_unit_multiplier() {
        for (value, unit, expected) in [(1, Second, 0), (-1, Second, 0), (3, Millisecond, 3), (3, Microsecond, 3_000), (3, Nanosecond, 3_000_000)] {
            assert_eq!(sleep_millis(&Value::Duration { value, unit }).unwrap(), expected);
        }
        assert_eq!(sleep_millis(&Value::Null).unwrap(), 0);
        assert_eq!(sleep_millis(&Value::Undefined).unwrap(), 0);
    }

    #[test]
    fn expression_clj_sleep_wraps_primitive_multiplication_before_negative_check() {
        for (value, unit) in [(-1, Millisecond), (-1, Microsecond), (i64::MAX, Microsecond)] {
            let error = sleep_millis(&Value::Duration { value, unit }).unwrap_err();
            assert_eq!(error.code, "xtdb/illegal-argument");
            assert_eq!(error.message.as_deref(), Some("timeout value is negative"));
        }
        for value in [i64::MIN, 1i64 << 61] {
            assert_eq!(sleep_millis(&Value::Duration { value, unit: Microsecond }).unwrap(), 0);
        }
    }
}
