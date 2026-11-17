use num_bigint::BigInt;
use rtdb_types::{AllocationScope,MemoryBudget,murmur3_32};
use rtdb_types::jvm_number::{Cast,Comparison,Dispatch,Number,decimal_parts_text};

#[test]
fn shared_decimal_text_retains_scale_edges_without_exponent_sized_output() {
    let memory = MemoryBudget::new(1 << 20);
    let scope = AllocationScope::new(&memory).unwrap();
    for (coefficient,scale,expected) in [
        (12300,4,"1.2300"),(1,6,"0.000001"),(1,7,"1E-7"),(-123,-1,"-1.23E+3"),
        (0,3,"0.000"),(0,-1,"0E+1"),(1,i32::MAX,"1E-2147483647"),(1,i32::MIN,"1E+2147483648"),
    ] {
        scope.reserve(4096).unwrap();
        assert_eq!(decimal_parts_text(&BigInt::from(coefficient),scale),expected);
    }
}

#[test]
fn shared_numeric_surface_retains_primitive_and_boxed_float_rounding() {
    let source = Number::Long(4_611_686_293_305_294_849);
    let Number::Float(direct) = Number::cast_primitive(Cast::Float,&source).unwrap() else { panic!("float") };
    let Number::Float(boxed) = Number::cast(Cast::Float,&source).unwrap() else { panic!("float") };
    assert_eq!(direct.to_bits(),0x5e80_0001);
    assert_eq!(boxed.to_bits(),0x5e80_0000);
}

#[test]
fn shared_numeric_comparison_remains_distinct_from_collection_and_wrapper_equality() {
    let memory = MemoryBudget::new(1 << 20);
    let scope = AllocationScope::new(&memory).unwrap();
    scope.reserve(4096).unwrap();
    let one = Number::Long(1);let floating = Number::Double(1.0);
    let decimal = Number::decimal("1.00").unwrap();let scaled = Number::decimal("1.0").unwrap();
    assert!(!one.equivalent(&floating));
    assert!(decimal.equivalent(&scaled));
    assert!(!decimal.java_equal(&scaled));
    for right in [&floating,&decimal] {
        scope.reserve(one.comparison_bound(right).unwrap()).unwrap();
        assert!(one.compare_numeric(Comparison::Equivalent,right,Dispatch::Object).unwrap());
    }
}

#[test]
fn shared_murmur3_surface_retains_the_existing_known_vectors() {
    assert_eq!(murmur3_32(b"",0),0);
    assert_eq!(murmur3_32(b"foo",0),0xf6a5_c420);
    assert_eq!(murmur3_32(b"hello",0),0x248b_fa47);
}
