use rtdb_types::{java_f32_text, java_f64_text};

#[test]
fn java_float_spelling_preserves_thresholds_signed_zero_and_subnormal_exponents() {
    for (value, expected) in [(0.0, "0.0"), (-0.0, "-0.0"), (1e-3, "0.001"), (1e-4, "1.0E-4"), (1e6, "1000000.0"), (1e7, "1.0E7"), (f64::from_bits(1), "4.9E-324"), (f64::from_bits(2), "9.9E-324"), (f64::INFINITY, "Infinity"), (f64::NEG_INFINITY, "-Infinity")] {
        assert_eq!(java_f64_text(value), expected);
    }
    assert_eq!(java_f64_text(f64::NAN), "NaN");
    assert_eq!(java_f32_text(0.1), "0.1");
    assert_eq!(java_f32_text(f32::from_bits(1)), "1.4E-45");
}
