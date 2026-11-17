use super::*;
use rtdb_columnar::MemoryBudget;

struct Witness { types:Vec<SchemaType>, reject:Option<SchemaType> }
impl IdentityWitness for Witness {
    fn type_hash(&mut self,kind:SchemaType,_:&mut ConversionContext)->Result<i32> {self.types.push(kind);if self.reject==Some(kind) {Err(invalid("Rejected Pair schema hash"))} else {Ok(101)}}
    fn builder_hash(&mut self,_:ObjectView<'_>,_:&mut ConversionContext)->Result<i32> {Err(invalid("Unexpected builder identity"))}
    fn byte_array_hash(&mut self,_:&Arc<ByteStorage>,_:&mut ConversionContext)->Result<i32> {Err(invalid("Unexpected byte identity"))}
    fn key_hash(&mut self,_:&JavaKey,_:&mut ConversionContext)->Result<i32> {Err(invalid("Unexpected key identity"))}
    fn pair_hash(&mut self,_:KeyIdentity,_:&mut ConversionContext)->Result<i32> {Err(invalid("Unexpected Pair identity"))}
}
fn schema(kind:Scalar,context:&mut ConversionContext)->SchemaHandle {let mut arena=SchemaObjectsBuilder::new(context).unwrap();let root=arena.scalar(kind,context).unwrap();let root=arena.build(root,context).unwrap();arena.finish(root,context).unwrap()}

#[test]
fn distinct_pair_objects_collapse_by_actual_ordinary_schema_key_equality() {
    let mut context=ConversionContext::new(MemoryBudget::new(usize::MAX));let a=schema(Scalar::I32,&mut context);let b=schema(Scalar::I32,&mut context);let c=schema(Scalar::I32,&mut context);let d=schema(Scalar::I32,&mut context);let first=Pair{left:a.view(),right:b.view(),identity:KeyIdentity::new().unwrap()};let second=Pair{left:c.view(),right:d.view(),identity:KeyIdentity::new().unwrap()};assert_ne!(first.identity,second.identity);let mut witness=Witness{types:Vec::new(),reject:None};let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let mut cache=Cache::new(None).unwrap();cache.put(first,false,&mut PairOps{depth:0},&mut evaluation).unwrap();assert!(matches!(cache.put_if_absent(second,true,|_|false,&mut PairOps{depth:0},&mut evaluation).unwrap(),PutOutcome::Present(&false)));assert_eq!(cache.len(),1);assert_eq!(cache.try_iter().unwrap().next().unwrap().0.identity,first.identity);assert_eq!(evaluation.witness.types,vec![SchemaType::I32;4]);
}

#[test]
fn pair_hash_failure_visits_left_before_right_and_preserves_only_the_successful_left_memo() {
    let mut context=ConversionContext::new(MemoryBudget::new(usize::MAX));let left=schema(Scalar::I32,&mut context);let right=schema(Scalar::I64,&mut context);let pair=Pair{left:left.view(),right:right.view(),identity:KeyIdentity::new().unwrap()};let mut witness=Witness{types:Vec::new(),reject:Some(SchemaType::I64)};let mut evaluation=Evaluation{context:&mut context,witness:&mut witness};let mut cache=Cache::new(None).unwrap();let cause=cache.put(pair,true,&mut PairOps{depth:0},&mut evaluation).unwrap_err();assert_eq!(cause.message.as_deref(),Some("Rejected Pair schema hash"));assert_eq!(evaluation.witness.types,vec![SchemaType::I32,SchemaType::I64]);let Object::Built{hash,..}=&left.arena.objects[left.object.index] else {panic!()};assert!(hash.get().is_some());let Object::Built{hash,..}=&right.arena.objects[right.object.index] else {panic!()};assert!(hash.get().is_none());assert!(cache.finish(&mut evaluation).is_err());
}
