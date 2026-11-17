use super::*;
use rtdb_columnar::MemoryBudget;
use super::super::{collections::JavaMapBuilder, hashing::{self, Comparable}};

#[derive(Debug,PartialEq,Eq)]
enum Event { Type(SchemaType), Builder(ObjectId), Bytes, Key(KeyIdentity), Pair(KeyIdentity) }
#[derive(Default)]
struct Witness {
    events:Vec<Event>, reject_type:Option<SchemaType>, reject_builder:bool, reject_bytes:bool,
    bytes:Vec<(Arc<ByteStorage>,i32)>, keys:Vec<(KeyIdentity,i32)>,
}
impl IdentityWitness for Witness {
    fn type_hash(&mut self,kind:SchemaType,_:&mut ConversionContext)->Result<i32> {self.events.push(Event::Type(kind));if self.reject_type==Some(kind) {Err(invalid("Rejected type observation"))} else {Ok(101)}}
    fn builder_hash(&mut self,builder:ObjectView<'_>,_:&mut ConversionContext)->Result<i32> {self.events.push(Event::Builder(builder.object_id()?));if self.reject_builder {Err(invalid("Rejected builder observation"))} else {Ok(57)}}
    fn byte_array_hash(&mut self,storage:&Arc<ByteStorage>,_:&mut ConversionContext)->Result<i32> {self.events.push(Event::Bytes);if self.reject_bytes {return Err(invalid("Rejected byte-array observation"));}Ok(self.bytes.iter().find_map(|(owner,hash)|Arc::ptr_eq(owner,storage).then_some(*hash)).unwrap_or(77))}
    fn key_hash(&mut self,key:&JavaKey,_:&mut ConversionContext)->Result<i32> {self.events.push(Event::Key(key.identity()));self.keys.iter().find_map(|(identity,hash)|(*identity==key.identity()).then_some(*hash)).ok_or_else(missing_identity)}
    fn pair_hash(&mut self,identity:KeyIdentity,_:&mut ConversionContext)->Result<i32> {self.events.push(Event::Pair(identity));Err(missing_identity())}
}
fn context()->ConversionContext {ConversionContext::new(MemoryBudget::new(usize::MAX))}
fn owner(context:&mut ConversionContext)->SchemaObjectsBuilder {SchemaObjectsBuilder::new(context).unwrap()}
fn key(value:JavaValue,owner:ValueOwner<'_>,payload:&Arc<MemoryReservation>,evaluation:&mut Evaluation<'_,impl IdentityWitness>)->JavaKey {JavaKey::new(value,owner,Arc::clone(payload),&mut *evaluation.context).unwrap()}

#[test]
fn primitive_hashes_keep_java_wrapper_classes_nan_signed_zero_and_utf16() {
    let mut context=context();let arena=owner(&mut context);let owner=arena.owner();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};
    for (value,expected) in [(JavaValue::Null,0),(JavaValue::Bool(true),1231),(JavaValue::Bool(false),1237),(JavaValue::I8(-1),-1),(JavaValue::I16(-32768),-32768),(JavaValue::I32(-9),-9),(JavaValue::I64(0x0102030405060708),67372044),(JavaValue::F32(f32::from_bits(0xffc00001)),0x7fc00000),(JavaValue::F32(-0.0),i32::MIN),(JavaValue::F64(f64::from_bits(0xfff8000000000001)),0x7ff80000),(JavaValue::F64(-0.0),i32::MIN),(JavaValue::String("A𐀀".into()),1832961),(JavaValue::Date(i64::MIN),i32::MIN)] {assert_eq!(evaluation.value_hash(&value,owner).unwrap(),expected);}
    assert!(evaluation.witness.events.is_empty());
}

#[test]
fn decimal_hashes_ignore_sign_extension_and_numeric_comparison_retains_scale_sensitive_equality() {
    let mut context=context();let arena=owner(&mut context);let owner=arena.owner();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};
    for (bytes,scale,expected) in [(vec![0,128],2,3970),(vec![255,128],2,-3966),(vec![255;12],3,-28),(vec![0;20],i32::MIN,i32::MIN)] {assert_eq!(evaluation.value_hash(&JavaValue::Decimal(BigDecimal::new(bytes,scale).unwrap()),owner).unwrap(),expected);}
    let left=JavaValue::Decimal(BigDecimal::new(vec![1],0).unwrap());let right=JavaValue::Decimal(BigDecimal::new(vec![10],1).unwrap());
    assert_eq!(hashing::compare(Comparable::Decimal,&left,&right,evaluation.context).unwrap(),0);assert!(!evaluation.values_equal(&left,owner,&right,owner,false).unwrap());
    let tiny=JavaValue::Decimal(BigDecimal::new(vec![1],i32::MAX).unwrap());let huge=JavaValue::Decimal(BigDecimal::new(vec![1],i32::MIN).unwrap());assert_eq!(hashing::compare(Comparable::Decimal,&tiny,&huge,evaluation.context).unwrap(),-1);
    let wide=JavaValue::Decimal(BigDecimal::new(vec![1;40],-200).unwrap());assert_eq!(hashing::compare(Comparable::Decimal,&wide,&huge,evaluation.context).unwrap(),-1);
}

#[test]
fn screened_comparable_operations_keep_nan_zero_unsigned_utf16_and_exact_wrapper_classes() {
    let mut context=context();
    for (class,left,right,expected) in [
        (Comparable::I8,JavaValue::I8(-128),JavaValue::I8(127),-255),
        (Comparable::I16,JavaValue::I16(-32768),JavaValue::I16(32767),-65535),
        (Comparable::F32,JavaValue::F32(-0.0),JavaValue::F32(0.0),-1),
        (Comparable::F32,JavaValue::F32(f32::from_bits(0xffc00001)),JavaValue::F32(f32::NAN),0),
        (Comparable::F64,JavaValue::F64(f64::NAN),JavaValue::F64(f64::INFINITY),1),
        (Comparable::F64,JavaValue::F64(-0.0),JavaValue::F64(0.0),-1),
        (Comparable::String,JavaValue::String("𐀀".into()),JavaValue::String("\u{e000}".into()),-2048),
        (Comparable::Date,JavaValue::Date(i64::MIN),JavaValue::Date(i64::MAX),-1),
        (Comparable::I64,JavaValue::I64(1),JavaValue::I32(1),0),
    ] {assert_eq!(hashing::compare(class,&left,&right,&mut context).unwrap(),expected);}
}

#[test]
fn byte_buffer_hashes_use_reversed_signed_remaining_bytes_without_identity_or_comparable_screening() {
    let mut context=context();let mut arena=owner(&mut context);let JavaValue::ByteArray(storage)=arena.byte_array(&[7,255,2,9],&mut context).unwrap() else {panic!()};let owner=arena.owner();
    let buffer=ByteBuffer::layout(storage,BufferBacking::Heap{offset:1,read_only:true},2,0,2).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};
    let value=JavaValue::ByteBuffer(buffer);assert_eq!(evaluation.value_hash(&value,owner).unwrap(),1022);assert!(hashing::comparable(&value).is_none());let key=key(value,owner,&evaluation.context.reserve(4096).unwrap(),&mut evaluation);assert_eq!(key.class_name(),"java.nio.HeapByteBufferR");assert!(evaluation.witness.events.is_empty());
}

#[test]
fn struct_hashes_visit_nested_list_bytes_before_schema_but_direct_byte_arrays_are_deep() {
    let mut context=context();let mut arena=SchemaObjectsBuilder::new(&mut context).unwrap();let field=arena.scalar(Scalar::Bytes,&mut context).unwrap();let array=arena.array(field,&mut context).unwrap();let id=arena.record(&mut context).unwrap();arena.field(id,"bytes",array,&mut context).unwrap();let direct_id=arena.record(&mut context).unwrap();arena.field(direct_id,"bytes",field,&mut context).unwrap();let bytes=arena.byte_array(&[1,2],&mut context).unwrap();let owner=arena.owner();let mut witness=Witness{reject_bytes:true,..Witness::default()};let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};
    let nested=JavaValue::Struct{schema:id,values:vec![JavaValue::Array(vec![bytes])]};assert!(evaluation.value_hash(&nested,owner).is_err());assert_eq!(evaluation.witness.events,vec![Event::Bytes]);
    let charge=evaluation.context.reserve(4096).unwrap();let storage=Arc::new(ByteStorage::new(vec![1,2],charge));evaluation.witness.events.clear();let direct=JavaValue::Struct{schema:direct_id,values:vec![JavaValue::ByteArray(storage)]};assert!(evaluation.value_hash(&direct,owner).is_ok());assert_eq!(evaluation.witness.events,vec![Event::Builder(direct_id)]);
}

#[test]
fn built_hash_memos_publish_only_full_success_and_preserve_successful_child_memos() {
    let mut context=context();let mut arena=SchemaObjectsBuilder::new(&mut context).unwrap();let root=arena.record(&mut context).unwrap();let child=arena.scalar(Scalar::I32,&mut context).unwrap();let child=arena.build(child,&mut context).unwrap();let recursive=arena.wrapper(root,true,&mut context).unwrap();arena.field(root,"a",child,&mut context).unwrap();arena.field(root,"next",recursive,&mut context).unwrap();let built=arena.build(root,&mut context).unwrap();let schema=arena.finish(built,&mut context).unwrap();
    let mut witness=Witness{reject_builder:true,..Witness::default()};let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};assert!(evaluation.schema_hash(schema.view()).is_err());assert_eq!(evaluation.witness.events,vec![Event::Type(SchemaType::Struct),Event::Type(SchemaType::I32),Event::Builder(root)]);
    let Object::Built{hash,..}=&schema.arena.objects[built.index] else {panic!()};assert!(hash.get().is_none());let Object::Built{hash,..}=&schema.arena.objects[child.index] else {panic!()};assert!(hash.get().is_some());
    evaluation.witness.events.clear();evaluation.witness.reject_builder=false;let expected=evaluation.schema_hash(schema.view()).unwrap();assert_eq!(evaluation.witness.events,vec![Event::Type(SchemaType::Struct),Event::Builder(root)]);evaluation.witness.events.clear();assert_eq!(evaluation.schema_hash(schema.view()).unwrap(),expected);assert!(evaluation.witness.events.is_empty());
    let mut imported=SchemaObjectsBuilder::new(evaluation.context).unwrap();let imported_root=imported.import(&schema,evaluation.context).unwrap();let imported=imported.finish(imported_root,evaluation.context).unwrap();assert_eq!(evaluation.schema_hash(imported.view()).unwrap(),expected);assert!(evaluation.witness.events.is_empty());
}

#[test]
fn long_map_keys_replace_values_while_retaining_the_first_shared_key_object() {
    let mut context=context();let arena=owner(&mut context);let owner=arena.owner();let payload=context.reserve(65536).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let first=key(JavaValue::I64(9),owner,&payload,&mut evaluation);let first_identity=first.identity();let shared=first.clone();let mut map=JavaMapBuilder::new(None,owner,Arc::clone(&payload)).unwrap();assert!(map.put(first,JavaValue::String("nine".into()),owner,&mut evaluation).unwrap().is_none());let replacement=key(JavaValue::I64(9),owner,&payload,&mut evaluation);assert!(matches!(map.put(replacement,JavaValue::String("latest".into()),owner,&mut evaluation).unwrap(),Some(previous) if matches!(previous.value(),JavaValue::String(value) if value=="nine")));let map=map.finish(&mut evaluation).unwrap();assert_eq!(map.len(),1);assert_eq!(map.entries().next().unwrap().0.identity(),first_identity);assert!(matches!(map.get(&shared,owner.arena,owner.arena,&mut evaluation,0).unwrap(),Some(JavaValue::String(value)) if value=="latest"));
}

#[test]
fn mixed_java_key_classes_and_null_are_normalized_without_cross_class_equality() {
    let mut context=context();let mut arena=owner(&mut context);let bytes=arena.byte_array(&[1],&mut context).unwrap();let JavaValue::ByteArray(storage)=&bytes else {panic!()};let mut witness=Witness::default();witness.bytes.push((Arc::clone(storage),1));let owner=arena.owner();let payload=context.reserve(131072).unwrap();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let mut map=JavaMapBuilder::new(Some(64),owner,Arc::clone(&payload)).unwrap();
    let values=vec![JavaValue::I8(1),JavaValue::I16(1),JavaValue::I32(1),JavaValue::I64(1),JavaValue::F32(f32::from_bits(1)),JavaValue::String("\u{1}".into()),JavaValue::Date(1),JavaValue::Decimal(BigDecimal::new(vec![1],-30).unwrap()),bytes,JavaValue::Array(vec![JavaValue::I32(-30)]),JavaValue::Null];
    for (index,value) in values.into_iter().enumerate() {let key=key(value,owner,&payload,&mut evaluation);assert!(map.put(key,JavaValue::I32(index as i32),owner,&mut evaluation).unwrap().is_none());}
    let map=map.finish(&mut evaluation).unwrap();assert_eq!(map.len(),11);assert!(!evaluation.witness.events.iter().any(|event|matches!(event,Event::Key(_))));
}

#[test]
fn null_map_values_recompute_key_hash_for_get_then_contains_key() {
    for null in [true,false] {
        let mut context=context();let mut arena=owner(&mut context);let bytes=arena.byte_array(&[1],&mut context).unwrap();let owner=arena.owner();let payload=context.reserve(65536).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let shared=key(bytes,owner,&payload,&mut evaluation);let mut maps=Vec::new();
        for _ in 0..2 {let mut map=JavaMapBuilder::new(None,owner,Arc::clone(&payload)).unwrap();map.put(shared.clone(),if null {JavaValue::Null} else {JavaValue::String("value".into())},owner,&mut evaluation).unwrap();maps.push(JavaValue::Map(map.finish(&mut evaluation).unwrap()));}
        evaluation.witness.events.clear();assert!(evaluation.values_equal(&maps[0],owner,&maps[1],owner,false).unwrap());assert_eq!(evaluation.witness.events.len(),if null {2} else {1});assert!(evaluation.witness.events.iter().all(|event|matches!(event,Event::Bytes)));
    }
}

#[test]
fn incoming_hash_failures_are_demanded_again_and_readonly_maps_recover() {
    let mut context=context();let mut arena=owner(&mut context);let bytes=arena.byte_array(&[1],&mut context).unwrap();let owner=arena.owner();let payload=context.reserve(65536).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let shared=key(bytes,owner,&payload,&mut evaluation);let mut map=JavaMapBuilder::new(None,owner,Arc::clone(&payload)).unwrap();map.put(shared.clone(),JavaValue::I32(1),owner,&mut evaluation).unwrap();let map=map.finish(&mut evaluation).unwrap();evaluation.witness.reject_bytes=true;assert!(map.get(&shared,owner.arena,owner.arena,&mut evaluation,0).is_err());evaluation.witness.reject_bytes=false;assert!(matches!(map.get(&shared,owner.arena,owner.arena,&mut evaluation,0).unwrap(),Some(JavaValue::I32(1))));
    let mut failed=JavaMapBuilder::new(None,owner,Arc::clone(&payload)).unwrap();failed.put(shared.clone(),JavaValue::I32(1),owner,&mut evaluation).unwrap();evaluation.witness.reject_bytes=true;assert!(failed.put(shared,JavaValue::I32(2),owner,&mut evaluation).is_err());assert!(failed.finish(&mut evaluation).is_err());
}

#[test]
fn typed_map_and_key_clones_keep_payload_and_allocation_leases_after_context_drop() {
    let memory=MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&memory));let arena=owner(&mut context);let owner=arena.owner();let payload=context.reserve(65536).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let retained=key(JavaValue::String("key".into()),owner,&payload,&mut evaluation);let mut map=JavaMapBuilder::new(None,owner,Arc::clone(&payload)).unwrap();map.put(retained.clone(),JavaValue::String("value".into()),owner,&mut evaluation).unwrap();let map=map.finish(&mut evaluation).unwrap();drop(evaluation);drop(payload);drop(arena);drop(context);assert!(memory.used_bytes()>0);assert_eq!(map.len(),1);drop(map);assert!(memory.used_bytes()>0);assert!(matches!(retained.value(),JavaValue::String(value) if value=="key"));drop(retained);assert_eq!(memory.used_bytes(),0);
}

#[test]
fn property_cache_hashing_precedes_metadata_short_circuits_and_self_identity_skips_it() {
    let mut context=context();let mut left=SchemaObjectsBuilder::new(&mut context).unwrap();let left_id=left.scalar(Scalar::I32,&mut context).unwrap();left.optional(left_id,true).unwrap();let left_id=left.build(left_id,&mut context).unwrap();let left=left.finish(left_id,&mut context).unwrap();let mut right=SchemaObjectsBuilder::new(&mut context).unwrap();let right_id=right.scalar(Scalar::I32,&mut context).unwrap();let right_id=right.build(right_id,&mut context).unwrap();let right=right.finish(right_id,&mut context).unwrap();let mut witness=Witness{reject_type:Some(SchemaType::I32),..Witness::default()};let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};assert!(evaluation.properties(left.view(),right.view()).is_err());assert_eq!(evaluation.witness.events,vec![Event::Type(SchemaType::I32)]);evaluation.witness.events.clear();assert!(evaluation.properties(left.view(),left.view()).unwrap());assert!(evaluation.witness.events.is_empty());
}

fn recursive(context:&mut ConversionContext)->(SchemaHandle,ObjectId) {let mut arena=SchemaObjectsBuilder::new(context).unwrap();let root=arena.record(context).unwrap();let next=arena.wrapper(root,true,context).unwrap();arena.field(root,"next",next,context).unwrap();let built=arena.build(root,context).unwrap();(arena.finish(built,context).unwrap(),root)}
#[test]
fn property_cache_uses_provisional_pairs_and_rehashes_live_builder_targets_at_each_put() {
    let mut context=context();let (left,left_builder)=recursive(&mut context);let (right,right_builder)=recursive(&mut context);let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};assert!(evaluation.properties(left.view(),right.view()).unwrap());assert_eq!(evaluation.witness.events.iter().filter(|event|matches!(event,Event::Type(SchemaType::Struct))).count(),2);assert_eq!(evaluation.witness.events.iter().filter(|event|matches!(event,Event::Builder(id) if *id==left_builder)).count(),4);assert_eq!(evaluation.witness.events.iter().filter(|event|matches!(event,Event::Builder(id) if *id==right_builder)).count(),4);assert!(!evaluation.witness.events.iter().any(|event|matches!(event,Event::Pair(_))));
}

#[test]
fn noncomparable_struct_keys_use_observed_ties_and_search_equal_replacements_before_requesting_a_new_tie() {
    let mut context=context();let mut arena=SchemaObjectsBuilder::new(&mut context).unwrap();let root=arena.record(&mut context).unwrap();let field=arena.scalar(Scalar::I64,&mut context).unwrap();let field=arena.build(field,&mut context).unwrap();arena.field(root,"n",field,&mut context).unwrap();let built=arena.build(root,&mut context).unwrap();let schema=arena.finish(built,&mut context).unwrap();let owner=schema.owner();let payload=context.reserve(131072).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let mut map=JavaMapBuilder::new(Some(64),owner,Arc::clone(&payload)).unwrap();let mut identities=Vec::new();
    for index in 0..9 {let value=JavaValue::Struct{schema:built,values:vec![JavaValue::I64((i64::from(index)<<32)|i64::from(index))]};let key=key(value,owner,&payload,&mut evaluation);identities.push(key.identity());evaluation.witness.keys.push((key.identity(),index+1));map.put(key,JavaValue::I32(index),owner,&mut evaluation).unwrap();}
    evaluation.witness.events.clear();let replacement=key(JavaValue::Struct{schema:built,values:vec![JavaValue::I64((1i64<<32)|1)]},owner,&payload,&mut evaluation);assert!(matches!(map.put(replacement.clone(),JavaValue::I32(99),owner,&mut evaluation).unwrap(),Some(previous) if matches!(previous.value(),JavaValue::I32(1))));let map=map.finish(&mut evaluation).unwrap();assert_eq!(map.len(),9);assert_eq!(map.entries().next().unwrap().0.identity(),identities[3]);assert!(matches!(map.get(&replacement,owner.arena,owner.arena,&mut evaluation,0).unwrap(),Some(JavaValue::I32(99))));assert!(!evaluation.witness.events.iter().any(|event|matches!(event,Event::Key(_))));
}

fn record_with_binary_default(context:&mut ConversionContext)->(SchemaHandle,Arc<ByteStorage>) {
    let mut arena=SchemaObjectsBuilder::new(context).unwrap();let bytes=arena.scalar(Scalar::Bytes,context).unwrap();let JavaValue::ByteArray(storage)=arena.byte_array(&[1,2],context).unwrap() else {panic!()};assert!(matches!(arena.default_with(bytes,4096,context,||Ok(JavaValue::ByteArray(Arc::clone(&storage)))).unwrap(),super::super::super::default_input::DefaultAttempt::Value(())));let bytes=arena.build(bytes,context).unwrap();let root=arena.record(context).unwrap();arena.field(root,"b",bytes,context).unwrap();let built=arena.build(root,context).unwrap();(arena.finish(built,context).unwrap(),storage)
}

#[test]
fn source_equal_struct_keys_with_different_schema_hashes_remain_separate_hash_filtered_mappings() {
    let mut context=context();let (left,left_storage)=record_with_binary_default(&mut context);let (right,right_storage)=record_with_binary_default(&mut context);let mut arena=SchemaObjectsBuilder::new(&mut context).unwrap();let imported_root=arena.import(&left,&mut context).unwrap();arena.import(&right,&mut context).unwrap();let family=arena.finish(imported_root,&mut context).unwrap();let owner=family.owner();assert_ne!(owner.arena.region,family.view().actual().unwrap().arena.region);let payload=context.reserve(131072).unwrap();let mut witness=Witness::default();witness.bytes.push((Arc::clone(&left_storage),11));witness.bytes.push((Arc::clone(&right_storage),22));let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let left_key=key(JavaValue::Struct{schema:left.object,values:vec![JavaValue::ByteArray(left_storage)]},left.owner(),&payload,&mut evaluation);let right_key=key(JavaValue::Struct{schema:right.object,values:vec![JavaValue::ByteArray(right_storage)]},right.owner(),&payload,&mut evaluation);
    assert!(evaluation.values_equal(left_key.value(),left.owner(),right_key.value(),right.owner(),false).unwrap());assert_ne!(evaluation.value_hash(left_key.value(),left.owner()).unwrap(),evaluation.value_hash(right_key.value(),right.owner()).unwrap());
    let mut map=JavaMapBuilder::new(Some(64),owner,Arc::clone(&payload)).unwrap();map.put(left_key.clone(),JavaValue::I32(1),owner,&mut evaluation).unwrap();map.put(right_key.clone(),JavaValue::I32(2),owner,&mut evaluation).unwrap();let map=map.finish(&mut evaluation).unwrap();assert_eq!(map.len(),2);assert!(matches!(map.get(&left_key,owner.arena,owner.arena,&mut evaluation,0).unwrap(),Some(JavaValue::I32(1))));assert!(matches!(map.get(&right_key,owner.arena,owner.arena,&mut evaluation,0).unwrap(),Some(JavaValue::I32(2))));
    let foreign=self::owner(evaluation.context);let foreign_view=foreign.owner();let mut rejected=JavaMapBuilder::new(None,foreign_view,Arc::clone(&payload)).unwrap();assert!(rejected.put(left_key,JavaValue::I32(3),foreign_view,&mut evaluation).is_err());assert!(rejected.finish(&mut evaluation).is_err());
}

#[test]
fn replaced_string_and_list_values_retain_their_payload_until_the_last_previous_value_drops() {
    let memory=MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&memory));let arena=owner(&mut context);let owner=arena.owner();let payload=context.reserve(65536).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let mut map=JavaMapBuilder::new(None,owner,Arc::clone(&payload)).unwrap();
    let first=key(JavaValue::I64(1),owner,&payload,&mut evaluation);assert!(map.put(first,JavaValue::String("first".into()),owner,&mut evaluation).unwrap().is_none());
    let replacement=key(JavaValue::I64(1),owner,&payload,&mut evaluation);let previous_string=map.put(replacement,JavaValue::Array(vec![JavaValue::String("inside".into())]),owner,&mut evaluation).unwrap().unwrap();
    let replacement=key(JavaValue::I64(1),owner,&payload,&mut evaluation);let previous_list=map.put(replacement,JavaValue::I32(3),owner,&mut evaluation).unwrap().unwrap();let map=map.finish(&mut evaluation).unwrap();drop(evaluation);drop(payload);drop(arena);drop(context);drop(map);
    assert!(memory.used_bytes()>0);assert!(matches!(previous_string.value(),JavaValue::String(value) if value=="first"));assert!(matches!(previous_list.value(),JavaValue::Array(values) if matches!(&values[0],JavaValue::String(value) if value=="inside")));drop(previous_string);assert!(memory.used_bytes()>0);assert!(matches!(previous_list.value(),JavaValue::Array(values) if values.len()==1));drop(previous_list);assert_eq!(memory.used_bytes(),0);
}

#[test]
fn importing_an_existing_root_then_its_larger_family_retains_the_family_and_additional_key_schema() {
    let memory=MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&memory));let (a,_)=record_with_binary_default(&mut context);let (b,storage)=record_with_binary_default(&mut context);let b_id=b.object;
    let mut f=SchemaObjectsBuilder::new(&mut context).unwrap();let f_root=f.import(&a,&mut context).unwrap();f.import(&b,&mut context).unwrap();let f=f.finish(f_root,&mut context).unwrap();let mut g=SchemaObjectsBuilder::new(&mut context).unwrap();let just_a=g.import(&a,&mut context).unwrap();let family=g.import(&f,&mut context).unwrap();assert_ne!(just_a,family);assert_eq!(g.import(&f,&mut context).unwrap(),family);let g=g.finish(family,&mut context).unwrap();assert_eq!(g.view().identity().unwrap(),a.view().identity().unwrap());
    let payload=context.reserve(65536).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let retained=key(JavaValue::Struct{schema:b_id,values:vec![JavaValue::ByteArray(storage)]},f.owner(),&payload,&mut evaluation);drop(f);drop(a);drop(b);let mut map=JavaMapBuilder::new(None,g.owner(),Arc::clone(&payload)).unwrap();map.put(retained.clone(),JavaValue::I32(9),g.owner(),&mut evaluation).unwrap();let map=map.finish(&mut evaluation).unwrap();drop(evaluation);drop(payload);drop(context);assert!(memory.used_bytes()>0);
    let mut fresh=ConversionContext::new(Arc::clone(&memory));g.retain(&mut fresh).unwrap();let mut evaluation=Evaluation{context:&mut fresh,witness:&mut witness};assert!(matches!(map.get(&retained,g.owner().arena,g.owner().arena,&mut evaluation,0).unwrap(),Some(JavaValue::I32(9))));drop(evaluation);drop(fresh);drop(map);drop(retained);drop(g);assert_eq!(memory.used_bytes(),0);
}

#[test]
fn the_same_retained_map_value_is_self_equal_through_distinct_valid_families_without_hashing() {
    let mut context=context();let (root,storage)=record_with_binary_default(&mut context);let mut f=SchemaObjectsBuilder::new(&mut context).unwrap();let imported=f.import(&root,&mut context).unwrap();let f=f.finish(imported,&mut context).unwrap();let mut g=SchemaObjectsBuilder::new(&mut context).unwrap();let imported=g.import(&f,&mut context).unwrap();let g=g.finish(imported,&mut context).unwrap();let payload=context.reserve(65536).unwrap();let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};
    let inner_key=key(JavaValue::ByteArray(storage),f.owner(),&payload,&mut evaluation);let mut inner=JavaMapBuilder::new(None,f.owner(),Arc::clone(&payload)).unwrap();inner.put(inner_key,JavaValue::Null,f.owner(),&mut evaluation).unwrap();let inner=JavaValue::Map(inner.finish(&mut evaluation).unwrap());let retained=key(inner,f.owner(),&payload,&mut evaluation);evaluation.witness.events.clear();evaluation.witness.reject_bytes=true;
    assert!(evaluation.values_equal(retained.value(),f.owner(),retained.value(),g.owner(),false).unwrap());assert!(evaluation.witness.events.is_empty());
}

#[test]
fn previous_value_retention_counts_parent_map_and_binary_owners_in_fresh_scopes_without_reserving_them_again() {
    let memory=MemoryBudget::new(usize::MAX);let mut context=ConversionContext::new(Arc::clone(&memory));let arena=owner(&mut context);let owner=arena.owner();let payload=context.reserve(65536).unwrap();let parent_bytes=payload.bytes();let inner_payload=context.reserve(4096).unwrap();let binary_charge=context.reserve(4096).unwrap();let binary_bytes=binary_charge.bytes();let storage=Arc::new(ByteStorage::new(vec![1,2,3],Arc::clone(&binary_charge)));let mut witness=Witness::default();let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};
    let mut inner=JavaMapBuilder::new(None,owner,Arc::clone(&inner_payload)).unwrap();let inner_key=key(JavaValue::String("key".into()),owner,&inner_payload,&mut evaluation);inner.put(inner_key,JavaValue::ByteBuffer(ByteBuffer::whole(storage)),owner,&mut evaluation).unwrap();let inner=JavaValue::Map(inner.finish(&mut evaluation).unwrap());
    let mut outer=JavaMapBuilder::new(None,owner,Arc::clone(&payload)).unwrap();let first=key(JavaValue::I64(1),owner,&payload,&mut evaluation);outer.put(first,JavaValue::Array(vec![inner]),owner,&mut evaluation).unwrap();let replacement=key(JavaValue::I64(1),owner,&payload,&mut evaluation);let previous=outer.put(replacement,JavaValue::I32(2),owner,&mut evaluation).unwrap().unwrap();let outer=outer.finish(&mut evaluation).unwrap();drop(evaluation);drop(payload);drop(inner_payload);drop(binary_charge);drop(arena);drop(context);drop(outer);
    let JavaValue::Array(values)=previous.value() else {panic!()};let JavaValue::Map(inner)=&values[0] else {panic!()};let mut probe=ConversionContext::new(Arc::clone(&memory));inner.retain(&mut probe).unwrap();let map_bytes=probe.allocation_limit()-probe.allocation_remaining();drop(probe);
    let mut denied_parent=ConversionContext::with_limit(Arc::clone(&memory),parent_bytes-1);assert!(previous.retain(&mut denied_parent).unwrap_err().code.ends_with("batch-too-large"));assert_eq!(denied_parent.allocation_remaining(),parent_bytes-1);drop(denied_parent);
    let mut denied_map=ConversionContext::with_limit(Arc::clone(&memory),parent_bytes);assert!(previous.retain(&mut denied_map).unwrap_err().code.ends_with("batch-too-large"));assert_eq!(denied_map.allocation_remaining(),0);drop(denied_map);
    let parent_and_map=parent_bytes.checked_add(map_bytes).unwrap();let mut denied_binary=ConversionContext::with_limit(Arc::clone(&memory),parent_and_map);assert!(previous.retain(&mut denied_binary).unwrap_err().code.ends_with("batch-too-large"));assert_eq!(denied_binary.allocation_remaining(),0);drop(denied_binary);
    let before=memory.used_bytes();let mut retained=ConversionContext::with_limit(Arc::clone(&memory),parent_and_map.checked_add(binary_bytes).unwrap());previous.retain(&mut retained).unwrap();assert_eq!(retained.allocation_remaining(),0);assert_eq!(memory.used_bytes(),before);previous.retain(&mut retained).unwrap();assert_eq!(retained.allocation_remaining(),0);assert_eq!(memory.used_bytes(),before);drop(previous);assert!(memory.used_bytes()>0);drop(retained);assert_eq!(memory.used_bytes(),0);
}
