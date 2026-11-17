use arrow_array::{Int16Array, Int32Array, Int64Array, NullArray};
use crate::MemoryBudget;
use super::*;

fn scalar(name: &str, data_type: DataType, nullable: bool, memory: &Arc<MemoryBudget>) -> ExecutionVector {
    let array: ArrayRef = match data_type {
        DataType::Int16 => Arc::new(Int16Array::from(vec![1])),
        DataType::Int32 => Arc::new(Int32Array::from(vec![1])),
        DataType::Int64 => Arc::new(Int64Array::from(vec![1])),
        DataType::Null => Arc::new(NullArray::new(1)),
        _ => panic!(),
    };
    let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
    ExecutionVector::with_reservation(Arc::new(Field::new(name, data_type, nullable)), array, reservation).unwrap()
}

fn descriptor(name: &str, data_type: DataType, nullable: bool, memory: &Arc<MemoryBudget>) -> Arc<VectorDescriptor> {
    Arc::clone(&scalar(name, data_type, nullable, memory).node().descriptor)
}

fn run(name: &str, ends: &Arc<VectorDescriptor>, values: &Arc<VectorDescriptor>, memory: &Arc<MemoryBudget>) -> Arc<VectorDescriptor> {
    VectorDescriptor::native_container(name, false, ContainerShapeRef::RunEnd { ends, values }, memory).unwrap()
}

fn failure(value: UnencodedBottom, width: RunEndWidth) -> VectorTypeFailure { VectorTypeFailure::UnsupportedRunEnd { value, width } }

#[test]
fn null_and_bottom_run_types_fail_without_hiding_field_or_reader_nullability() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    for (data_type, width, width_text) in [(DataType::Int16, RunEndWidth::I16, "I16"), (DataType::Int32, RunEndWidth::I32, "I32"), (DataType::Int64, RunEndWidth::I64, "I64")] {
        let ends = descriptor("ends", data_type, false, &memory);
        for (nullable, value, value_text) in [(true, UnencodedBottom::Null, "Null"), (false, UnencodedBottom::Nothing, "Nothing")] {
            let values = descriptor("values", DataType::Null, nullable, &memory);
            let runs = run("runs", &ends, &values, &memory);
            let before = memory.used_bytes();
            for _ in 0..3 {
                assert_eq!(runs.logical_type(), Err(failure(value, width)));
                assert_eq!(memory.used_bytes(), before);
            }
            assert_eq!(runs.logical_type().unwrap_err().to_string(), format!("{value_text} can't be laid out as RunEnd(width={width_text})"));
            assert!(!runs.is_nullable());
            assert_eq!(runs.reader_nullable(), nullable);
            let projected = runs.arrow_field().unwrap();
            assert!(!projected.field().is_nullable());
            let DataType::RunEndEncoded(ends_field, values_field) = projected.field().data_type() else { panic!() };
            assert_eq!(ends_field.data_type(), ends.scalar_field().unwrap().data_type());
            assert_eq!(values_field.data_type(), &DataType::Null);
            assert_eq!(values_field.is_nullable(), nullable);
            assert!(crate::from_field(projected.field()).is_err());
        }
    }
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn container_type_failures_follow_child_order_without_rewriting_inner_width() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let ends16 = descriptor("ends16", DataType::Int16, false, &memory);
    let ends64 = descriptor("ends64", DataType::Int64, false, &memory);
    let null = descriptor("null", DataType::Null, true, &memory);
    let bottom = descriptor("bottom", DataType::Null, false, &memory);
    let first = run("z-first", &ends16, &null, &memory);
    let second = run("a-second", &ends64, &bottom, &memory);
    for children in [[Arc::clone(&first), Arc::clone(&second)], [Arc::clone(&second), Arc::clone(&first)]] {
        let expected = children[0].logical_type().unwrap_err();
        let fields = [StructFieldRef { key: children[0].name(), descriptor: &children[0] }, StructFieldRef { key: children[1].name(), descriptor: &children[1] }];
        for shape in [ContainerShapeRef::Struct(&fields), ContainerShapeRef::OrdinalUnion(&children)] {
            let parent = VectorDescriptor::native_container("parent", false, shape, &memory).unwrap();
            assert_eq!(parent.logical_type(), Err(expected));
            assert!(parent.arrow_field().is_ok());
        }
    }
    for shape in [
        ContainerShapeRef::List(&first), ContainerShapeRef::Set(&first),
        ContainerShapeRef::FixedList { size: 2, elements: &first },
        ContainerShapeRef::Map { sorted: true, entries: &first },
        ContainerShapeRef::RunEnd { ends: &ends64, values: &first },
    ] {
        let parent = VectorDescriptor::native_container("parent", true, shape, &memory).unwrap();
        assert_eq!(parent.logical_type(), Err(failure(UnencodedBottom::Null, RunEndWidth::I16)));
        assert!(parent.arrow_field().is_ok());
    }
    drop((ends16, ends64, null, bottom, first, second));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn run_type_replaces_only_outer_encoding_and_ends_admission_uses_reader_metadata() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let ends16 = descriptor("ends16", DataType::Int16, false, &memory);
    let ends32 = descriptor("ends32", DataType::Int32, false, &memory);
    let ends64 = descriptor("ends64", DataType::Int64, false, &memory);
    let values = descriptor("values", DataType::Int32, true, &memory);
    let first = run("inner", &ends16, &values, &memory);
    let second = run("outer", &ends64, &first, &memory);
    assert_eq!(second.logical_type().unwrap(), &LogicalType::RunEndEncoded {
        width: RunEndWidth::I64, value: Box::new(LogicalType::Scalar(rtdb_types::ScalarType::I32).maybe()),
    });
    assert!(!second.is_nullable());
    assert!(second.reader_nullable());
    let encoded_ends = run("encoded-ends", &ends16, &ends32, &memory);
    let third = run("third", &encoded_ends, &values, &memory);
    assert_eq!(third.logical_type().unwrap(), &LogicalType::RunEndEncoded {
        width: RunEndWidth::I32, value: Box::new(LogicalType::Scalar(rtdb_types::ScalarType::I32).maybe()),
    });
    assert!(VectorDescriptor::native_container("bad-ends", false, ContainerShapeRef::RunEnd { ends: &first, values: &values }, &memory).is_err());
    let nested_element = VectorDescriptor::native_container("list", false, ContainerShapeRef::List(&first), &memory).unwrap();
    let outer = run("outer-list", &ends32, &nested_element, &memory);
    let LogicalType::RunEndEncoded { value, .. } = outer.logical_type().unwrap() else { panic!() };
    let LogicalType::List { element, .. } = value.as_ref() else { panic!() };
    assert_eq!(element.as_ref(), first.logical_type().unwrap());
    drop((ends16, ends32, ends64, values, first, second, encoded_ends, third, nested_element, outer));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn integer_backed_extensions_remain_invalid_run_ends_through_encoded_readers() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let ends = descriptor("ends", DataType::Int32, false, &memory);
    let values = descriptor("values", DataType::Int64, false, &memory);
    for scalar_type in [ScalarType::Oid, ScalarType::RegClass, ScalarType::RegProc] {
        let field = crate::to_field("run_ends", &LogicalType::Scalar(scalar_type)).unwrap();
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
        let extension_vector = ExecutionVector::with_reservation(Arc::new(field), array, reservation).unwrap();
        let extension = Arc::clone(&extension_vector.node().descriptor);
        assert_eq!(extension.scalar_field().unwrap().data_type(), &DataType::Int32);
        assert!(!extension.reader_nullable());
        let encoded = run("encoded", &ends, &extension, &memory);
        assert!(!encoded.reader_nullable());
        for candidate in [&extension, &encoded] {
            let before = memory.used_bytes();
            let owners = Arc::strong_count(candidate);
            assert!(VectorDescriptor::native_container("rejected", false, ContainerShapeRef::RunEnd { ends: candidate, values: &values }, &memory).is_err());
            assert_eq!(memory.used_bytes(), before);
            assert_eq!(Arc::strong_count(candidate), owners);
        }
        let values_vector = scalar("values", DataType::Int64, false, &memory);
        let field = Field::new("runs", DataType::RunEndEncoded(
            Arc::new(extension.scalar_field().unwrap().clone()),
            Arc::new(values_vector.descriptor().scalar_field().unwrap().clone()),
        ), false);
        let before = memory.used_bytes();
        assert_eq!(ExecutionVector::from_physical(Arc::new(field), PhysicalLayout::RunEndEncoded {
            length: 1, run_ends: extension_vector.clone(), values: values_vector.clone(),
        }, &memory).unwrap_err().code, "rtdb/invalid-arrow");
        assert_eq!(memory.used_bytes(), before);
    }
    drop((ends, values));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn native_extension_fields_retain_empty_serialized_extension_metadata() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let child = descriptor("element", DataType::Int32, false, &memory);
    for (shape, extension) in [(ContainerShapeRef::Set(&child), "set"), (ContainerShapeRef::TimestampRange(&child), "xt/tstz-range")] {
        let descriptor = VectorDescriptor::native_container("extension", false, shape, &memory).unwrap();
        let expected = HashMap::from([
            (crate::EXTENSION_NAME_KEY.to_owned(), extension.to_owned()),
            (crate::EXTENSION_METADATA_KEY.to_owned(), String::new()),
        ]);
        assert_eq!(descriptor.metadata(), &expected);
        let projected = descriptor.arrow_field().unwrap();
        assert_eq!(projected.field().metadata(), &expected);
    }
    drop(child);
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn timestamp_range_type_is_constant_while_its_field_retains_the_actual_child() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let ends = descriptor("ends", DataType::Int32, false, &memory);
    let null = descriptor("values", DataType::Null, true, &memory);
    let child = run("custom-child", &ends, &null, &memory);
    let range = VectorDescriptor::native_container("range", true, ContainerShapeRef::TimestampRange(&child), &memory).unwrap();
    assert!(child.logical_type().is_err());
    assert_eq!(range.logical_type().unwrap(), &LogicalType::List {
        kind: ListKind::TimestampTzRange,
        element: Box::new(LogicalType::Scalar(rtdb_types::ScalarType::Timestamp { unit: rtdb_types::TimeUnit::Microsecond, timezone: Some("UTC".to_owned()) })),
    }.maybe());
    let projected = range.arrow_field().unwrap();
    assert_eq!(projected.field().metadata().get(crate::EXTENSION_NAME_KEY).map(String::as_str), Some("xt/tstz-range"));
    let DataType::FixedSizeList(element, 2) = projected.field().data_type() else { panic!() };
    assert_eq!(element.name(), "custom-child");
    assert!(matches!(element.data_type(), DataType::RunEndEncoded(_, _)));
    drop((ends, null, child, range, projected));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn union_reader_nullability_remains_false_when_its_logical_type_admits_null() {
    let memory = MemoryBudget::new(8 * 1024 * 1024);
    let integer = descriptor("integer", DataType::Int32, false, &memory);
    let null = descriptor("null", DataType::Null, true, &memory);
    let children = [Arc::clone(&integer), Arc::clone(&null)];
    let union = VectorDescriptor::ordinal_union("union", &children, &memory).unwrap();
    assert!(!union.reader_nullable());
    assert!(!union.is_nullable());
    assert!(union.logical_type().unwrap().nullable());
    let runs = run("runs", &integer, &union, &memory);
    assert!(!runs.reader_nullable());
    assert!(!runs.is_nullable());
    assert!(runs.logical_type().unwrap().nullable());
    drop((integer, null, children, union, runs));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn negative_native_fixed_width_is_representable_metadata_but_not_external_ingress() {
    let memory = MemoryBudget::new(4 * 1024 * 1024);
    let child = descriptor("element", DataType::Int32, false, &memory);
    let fixed = VectorDescriptor::native_container("fixed", false, ContainerShapeRef::FixedList { size: -3, elements: &child }, &memory).unwrap();
    assert_eq!(fixed.logical_type().unwrap(), &LogicalType::List {
        kind: ListKind::FixedSizeList(-3), element: Box::new(LogicalType::Scalar(rtdb_types::ScalarType::I32)),
    });
    let field = fixed.arrow_field().unwrap();
    assert!(matches!(field.field().data_type(), DataType::FixedSizeList(_, -3)));
    assert!(crate::from_field(field.field()).is_err());
    drop((child, fixed, field));
    assert_eq!(memory.used_bytes(), 0);
}

fn node(descriptor: Arc<VectorDescriptor>, length: usize, storage: NodeLayout) -> ExecutionVector {
    let owner = Arc::new(NodeOwner { backing: BackingOwner::Native, metadata: Arc::clone(&descriptor.reservation) });
    ExecutionVector::direct(Arc::new(PhysicalNode { declaration: Declaration::Native, descriptor, length, storage, owner }))
}

#[test]
fn concrete_reads_and_structural_materialization_do_not_demand_failed_logical_metadata() {
    let memory = MemoryBudget::new(16 * 1024 * 1024);
    let ends = scalar("ends", DataType::Int32, false, &memory).into_direct().unwrap();
    let values = scalar("values", DataType::Null, true, &memory).into_direct().unwrap();
    let runs_descriptor = run("runs", &ends.descriptor, &values.descriptor, &memory);
    let runs = node(runs_descriptor, 1, NodeLayout::RunEnd(RunEndReader::new(RunStorage { logical_offset: 0, ends, values })));
    assert_eq!(runs.logical_type(), Err(failure(UnencodedBottom::Null, RunEndWidth::I32)));
    assert!(runs.reader_nullable());
    assert!(runs.cell(0).unwrap().is_null().unwrap());
    assert_eq!(runs.cell(0).unwrap().object().unwrap().value(), &Value::Null);
    let Err(VectorReadError::Anomaly(error)) = runs.cell(0).unwrap().get_long(NullChecks::Enabled) else { panic!() };
    assert_eq!(error.code, "rtdb/unsupported-vector-reader");
    let child = runs.into_direct().unwrap();
    let children = [Arc::clone(&child.descriptor)];
    let fields = [StructFieldRef { key: children[0].name(), descriptor: &children[0] }];
    let parent_descriptor = VectorDescriptor::native_container("parent", false, ContainerShapeRef::Struct(&fields), &memory).unwrap();
    let parent = node(parent_descriptor, 1, NodeLayout::Struct { validity: None, children: vec![Arc::clone(&child)] });
    assert!(parent.logical_type().is_err());
    assert!(!parent.reader_nullable());
    let named = parent.vector_for("runs").unwrap().unwrap();
    let selected = named.select(&Selection::All).unwrap();
    assert!(named.reader_nullable());
    assert!(selected.reader_nullable());
    assert_eq!(selected.logical_type(), Err(failure(UnencodedBottom::Null, RunEndWidth::I32)));
    let Err(VectorReadError::Anomaly(error)) = parent.cell(0).unwrap().get_int(NullChecks::Disabled) else { panic!() };
    assert_eq!(error.code, "rtdb/unsupported-vector-reader");
    let Err(VectorReadError::Anomaly(error)) = parent.cell(0).unwrap().get_bytes() else { panic!() };
    assert_eq!(error.code, "rtdb/unsupported-vector-reader");
    assert_eq!(parent.cell(0).unwrap().object().unwrap().value(), &Value::Struct(BTreeMap::from([("runs".to_owned(), Value::Null)])));
    let list_descriptor = VectorDescriptor::native_container("list", false, ContainerShapeRef::FixedList { size: 1, elements: &child.descriptor }, &memory).unwrap();
    let list = node(list_descriptor, 1, NodeLayout::FixedList { validity: None, size: 1, elements: child });
    assert!(list.logical_type().is_err());
    assert_eq!(list.cell(0).unwrap().object().unwrap().value(), &Value::FixedSizeList(vec![Value::Null]));
    drop((children, parent, list, named, selected));
    assert_eq!(memory.used_bytes(), 0);
}

#[test]
fn failed_metadata_parent_admission_keeps_child_owners_and_releases_retry() {
    let child_memory = MemoryBudget::new(4 * 1024 * 1024);
    let parent_memory = MemoryBudget::new(1024 * 1024);
    let ends = descriptor("ends", DataType::Int32, false, &child_memory);
    let null = descriptor("null", DataType::Null, true, &child_memory);
    let child = run("child", &ends, &null, &child_memory);
    let before = child_memory.used_bytes();
    let owners = Arc::strong_count(&child);
    let blocker = parent_memory.reserve(parent_memory.limit_bytes()).unwrap();
    assert!(VectorDescriptor::native_container("parent", false, ContainerShapeRef::List(&child), &parent_memory).is_err());
    assert_eq!(child_memory.used_bytes(), before);
    assert_eq!(Arc::strong_count(&child), owners);
    drop(blocker);
    assert_eq!(parent_memory.used_bytes(), 0);
    let parent = VectorDescriptor::native_container("parent", false, ContainerShapeRef::List(&child), &parent_memory).unwrap();
    assert_eq!(parent.logical_type(), child.logical_type());
    assert!(parent_memory.used_bytes() > 0);
    drop((ends, null, child));
    assert!(child_memory.used_bytes() > 0);
    drop(parent);
    assert_eq!(child_memory.used_bytes(), 0);
    assert_eq!(parent_memory.used_bytes(), 0);
}

#[test]
fn native_struct_reserves_cloned_child_names_before_logical_derivation() {
    let child_memory = MemoryBudget::new(4 * 1024 * 1024);
    let parent_memory = MemoryBudget::new(64 * 1024);
    let child = descriptor(&"x".repeat(128 * 1024), DataType::Int32, false, &child_memory);
    let before = child_memory.used_bytes();
    let owners = Arc::strong_count(&child);
    let fields = [StructFieldRef { key: child.name(), descriptor: &child }];
    assert!(VectorDescriptor::native_container("parent", false, ContainerShapeRef::Struct(&fields), &parent_memory).is_err());
    assert_eq!(parent_memory.used_bytes(), 0);
    assert_eq!(child_memory.used_bytes(), before);
    assert_eq!(Arc::strong_count(&child), owners);
    drop(child);
    assert_eq!(child_memory.used_bytes(), 0);
}
