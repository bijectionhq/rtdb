use super::*;
use super::super::{FixedBytesLimit, TokenBuffer, TokenRecycler};
use crate::connect::registry::reader_symbols::{FactoryProfile, Profile, SymbolName};
use rtdb_columnar::MemoryBudget;
use rtdb_types::Value;
use std::sync::Mutex;

#[derive(Clone)]
struct Intern(Arc<Mutex<InternState>>);
struct InternState {
    names: Vec<Arc<SymbolName>>,
    calls: usize,
    storage: Vec<Arc<MemoryReservation>>,
    control: Arc<MemoryReservation>,
}
impl Intern {
    fn new(context: &mut ConversionContext) -> Self {
        let control = context.reserve(4096).unwrap();
        Self(Arc::new(Mutex::new(InternState { names: Vec::new(), calls: 0, storage: Vec::new(), control })))
    }
    fn calls(&self) -> usize { self.0.lock().unwrap().calls }
}
impl InternAuthority for Intern {
    fn intern(&mut self, candidate: Arc<SymbolName>, context: &mut ConversionContext) -> Result<Arc<SymbolName>> {
        let mut state = self.0.lock().unwrap();
        context.retain(Arc::clone(&state.control))?;
        for charge in &state.storage { context.retain(Arc::clone(charge))?; }
        state.calls += 1;
        if let Some(name) = state.names.iter().find(|name| name.text().view().units().eq(candidate.text().view().units())) {
            return Ok(Arc::clone(name));
        }
        state.storage.push(context.reserve(1024)?);
        state.names.push(Arc::clone(&candidate));
        Ok(candidate)
    }
}

#[derive(Clone)]
struct BufferReceipt(Arc<Mutex<BufferStats>>);
struct BufferStats { capacities: Vec<usize>, closes: usize, control: Arc<MemoryReservation> }
impl BufferReceipt {
    fn new(context: &mut ConversionContext) -> Self {
        let control = context.reserve(4096).unwrap();
        let mut capacities = Vec::new();
        capacities.try_reserve_exact(16).unwrap();
        Self(Arc::new(Mutex::new(BufferStats { capacities, closes: 0, control })))
    }
    fn closes(&self) -> usize { self.0.lock().unwrap().closes }
}
struct Buffers { pool: TokenRecycler, receipt: BufferReceipt }
impl InputBuffers for Buffers {
    fn profile(&self) -> Profile { self.pool.profile() }
    fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> {
        let mut state = self.receipt.0.lock().unwrap();
        context.retain(Arc::clone(&state.control))?;
        let buffer = self.pool.take(minimum, context)?;
        assert!(state.capacities.len() < state.capacities.capacity());
        state.capacities.push(buffer.len());
        Ok(buffer)
    }
    fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()> {
        let mut state = self.receipt.0.lock().unwrap();
        context.retain(Arc::clone(&state.control))?;
        state.closes += 1;
        self.pool.give(buffer, context)
    }
}
fn policy() -> SourcePolicy { SourcePolicy { validate_defaults: false, fixed_bytes: FixedBytesLimit::DEFAULT } }
fn factory(context: &mut ConversionContext) -> SchemaStringFactory {
    SchemaStringFactory::new_default(FactoryProfile { profile: Profile::Jackson221, identity_hash_seed: 37 }, context).unwrap()
}
fn runtime(context: &mut ConversionContext) -> (AvroSchemaRuntime, Intern, BufferReceipt) {
    let intern = Intern::new(context);
    let receipt = BufferReceipt::new(context);
    let buffers = Buffers { pool: TokenRecycler::empty(Profile::Jackson221), receipt: receipt.clone() };
    (AvroSchemaRuntime::new(factory(context), buffers, intern.clone(), context).unwrap(), intern, receipt)
}

#[test]
fn repeated_groups_reuse_the_supplied_factory_recycler_and_intern_owner() {
    let mut context = ConversionContext::new(MemoryBudget::new(usize::MAX));
    let factory = factory(&mut context);
    let intern = Intern::new(&mut context);
    let receipt = BufferReceipt::new(&mut context);
    let buffers = Buffers { pool: TokenRecycler::empty(Profile::Jackson221), receipt: receipt.clone() };
    let mut runtime = AvroSchemaRuntime::new(factory.clone(), buffers, intern.clone(), &mut context).unwrap();
    context.reserve(16_384).unwrap();
    let mut document = r#"{"type":"record","name":"R","fields":[]}"#.to_owned();
    document.extend(std::iter::repeat_n(' ', 6000 - document.len()));
    let first = runtime.parse(Some(JavaText::from_utf8(&document)), &[], &BTreeMap::new(), policy(), &mut context).unwrap().unwrap();
    let calls = intern.calls();
    assert!(calls > 0);
    let second = runtime.parse(Some(JavaText::from_utf8(r#"{"type":"record","name":"R","fields":[]}"#)), &[], &BTreeMap::new(), policy(), &mut context).unwrap().unwrap();
    assert_eq!(intern.calls(), calls);
    assert!(!first.paired_root().unwrap().canonical().unwrap().same_object(&second.paired_root().unwrap().canonical().unwrap()));
    assert_eq!(receipt.0.lock().unwrap().capacities, [6000, 6000]);
    let other_buffers = TokenRecycler::empty(Profile::Jackson221);
    let mut other = AvroSchemaRuntime::new(factory, other_buffers, intern.clone(), &mut context).unwrap();
    other.parse(Some(JavaText::from_utf8(r#"{"type":"record","name":"R","fields":[]}"#)), &[], &BTreeMap::new(), policy(), &mut context).unwrap().unwrap();
    assert_eq!(intern.calls(), calls);
    assert_eq!(receipt.closes(), 2);
}

#[test]
fn null_root_parses_references_but_never_demands_native_transport() {
    let mut context = ConversionContext::new(MemoryBudget::new(usize::MAX));
    let (mut runtime, _, receipt) = runtime(&mut context);
    let unrepresentable = JavaText::from_utf8(r#"{"type":"record","name":"R","doc":"\ud800","fields":[]}"#);
    assert!(runtime.parse(None, &[unrepresentable], &BTreeMap::new(), policy(), &mut context).unwrap().is_none());
    assert_eq!(receipt.closes(), 1);
    assert!(runtime.parse(None, &[], &BTreeMap::new(), policy(), &mut context).unwrap().is_none());
    assert_eq!(receipt.closes(), 1);
    let failure = runtime.parse(None, &[JavaText::from_utf8("\"Missing\"")], &BTreeMap::new(), policy(), &mut context).err().unwrap();
    assert!(failure.code.ends_with("avro-schema"));
    assert_eq!(receipt.closes(), 2);
    let failure = runtime.parse(Some(JavaText::from_utf8("")), &[], &BTreeMap::new(), policy(), &mut context).err().unwrap();
    assert!(failure.code.ends_with("avro-schema"));
    assert_eq!(receipt.closes(), 3);
    let failure = runtime.parse(Some(JavaText::from_utf8("\"R\"")), &[unrepresentable], &BTreeMap::new(), policy(), &mut context).err().unwrap();
    assert!(failure.code.ends_with("avro-reader-native-projection"));
    assert_eq!(receipt.closes(), 5);
}

#[test]
fn null_root_keeps_reference_schema_and_dangling_failures_with_close() {
    let mut context = ConversionContext::new(MemoryBudget::new(usize::MAX));
    let (mut runtime, _, receipt) = runtime(&mut context);
    let failure = runtime.parse(None, &[JavaText::from_utf8("\"Missing\" tail")], &BTreeMap::new(), policy(), &mut context).err().unwrap();
    assert!(failure.code.ends_with("avro-schema"));
    let failure = runtime.parse(None, &[JavaText::from_utf8("\"int\" tail")], &BTreeMap::new(), policy(), &mut context).err().unwrap();
    assert!(failure.code.ends_with("avro-schema-dangling"));
    assert_eq!(receipt.closes(), 2);
}

#[test]
fn default_validation_policy_belongs_to_each_fresh_source_parser_group() {
    let mut context = ConversionContext::new(MemoryBudget::new(usize::MAX));
    let (mut runtime, _, receipt) = runtime(&mut context);
    let reference = JavaText::from_utf8(r#"{"type":"record","name":"R","fields":[{"name":"x","type":"int","default":null}]}"#);
    assert!(runtime.parse(None, &[reference], &BTreeMap::new(), policy(), &mut context).unwrap().is_none());
    let validating = SourcePolicy { validate_defaults: true, ..policy() };
    let failure = runtime.parse(None, &[reference], &BTreeMap::new(), validating, &mut context).err().unwrap();
    assert_eq!(failure.message.as_deref(), Some("Invalid Avro field default"));
    assert!(runtime.parse(None, &[reference], &BTreeMap::new(), policy(), &mut context).unwrap().is_none());
    assert_eq!(receipt.closes(), 3);
}

#[test]
fn owner_admission_fails_before_authority_use_and_fresh_context_retention_can_recover() {
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let source_factory = factory(&mut context);
    let intern = Intern::new(&mut context);
    let receipt = BufferReceipt::new(&mut context);
    let buffers = Buffers { pool: TokenRecycler::empty(Profile::Jackson221), receipt: receipt.clone() };
    let mut denied = ConversionContext::with_limit(Arc::clone(&budget), 0);
    assert!(AvroSchemaRuntime::new(source_factory.clone(), buffers, intern.clone(), &mut denied).is_err());
    assert_eq!(intern.calls(), 0);
    assert_eq!(receipt.closes(), 0);
    assert!(receipt.0.lock().unwrap().capacities.is_empty());
    let buffers = Buffers { pool: TokenRecycler::empty(Profile::Jackson221), receipt: receipt.clone() };
    let mut runtime = AvroSchemaRuntime::new(source_factory, buffers, intern.clone(), &mut context).unwrap();
    drop(context);
    assert!(runtime.parse(Some(JavaText::from_utf8("\"int\"")), &[], &BTreeMap::new(), policy(), &mut denied).is_err());
    assert_eq!(receipt.closes(), 0);
    let mut next = ConversionContext::new(Arc::clone(&budget));
    assert!(runtime.parse(Some(JavaText::from_utf8("\"int\"")), &[], &BTreeMap::new(), policy(), &mut next).unwrap().is_some());
    assert!(next.retains(&runtime.charge));
    assert_eq!(receipt.closes(), 1);
    drop(runtime);
    drop(intern);
    drop(receipt);
    drop(next);
    drop(denied);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn exact_utf16_datum_survives_runtime_input_and_construction_context_drop() {
    use super::super::java_schema::Text;
    let budget = MemoryBudget::new(usize::MAX);
    let mut context = ConversionContext::new(Arc::clone(&budget));
    let (mut runtime, intern, receipt) = runtime(&mut context);
    let reference = Text::copy(JavaText::from_utf8(r#"{"type":"record","name":"ns.R","doc":"😀","fields":[{"name":"x","type":"int"}]}"#), &mut context).unwrap();
    let root = Text::copy(JavaText::from_utf16(&[34, 110, 115, 46, 82, 34]), &mut context).unwrap();
    let datum = runtime.parse(Some(root.view()), &[reference.view()], &BTreeMap::new(), policy(), &mut context).unwrap().unwrap();
    let wrapper = context.reserve(size_of::<AvroDatumPlan>().checked_mul(16).unwrap() + 4096).unwrap();
    let datum = Arc::new(datum);
    let connect = datum.connect_plan(None, None, &mut 0, &mut context).unwrap();
    drop(root);
    drop(reference);
    drop(runtime);
    drop(intern);
    drop(receipt);
    drop(context);
    assert!(budget.used_bytes() > 0);
    let paired = connect.datum.paired_root().unwrap().canonical().unwrap();
    assert!(paired.is_record());
    drop(paired);
    let schema = connect.connect.resolved();
    assert_eq!(schema.doc, None);
    assert_eq!(schema.parameters["io.confluent.connect.avro.record.doc"], "😀");
    let mut next = ConversionContext::new(Arc::clone(&budget));
    let converted = connect.decode(&[14], &mut next).unwrap();
    assert_eq!(converted.value, Value::Struct(BTreeMap::from([("x".into(), Value::I32(7))])));
    drop(converted);
    drop(datum);
    drop(connect);
    drop(wrapper);
    drop(next);
    assert_eq!(budget.used_bytes(), 0);
}

#[test]
fn native_open_and_token_return_denials_detach_without_source_cleanup_and_allow_fresh_context_retry() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct FailureState {
        stage: AtomicUsize,
        acquires: AtomicUsize,
        gives: AtomicUsize,
        closes: AtomicUsize,
        abandons: AtomicUsize,
        charge: Arc<MemoryReservation>,
    }
    struct FailOnce { inner: pool::ThreadInputBuffers, state: Arc<FailureState> }
    impl InputBuffers for FailOnce {
        fn profile(&self) -> Profile { self.inner.profile() }
        fn acquire(&mut self, context: &mut ConversionContext) -> Result<()> {
            context.retain(Arc::clone(&self.state.charge))?;
            self.inner.acquire(context)?;
            self.state.acquires.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
        fn take(&mut self, minimum: usize, context: &mut ConversionContext) -> Result<TokenBuffer> {
            if self.state.stage.compare_exchange(1, 0, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                let denied = context.allocation_remaining().checked_add(1).unwrap();
                context.reserve(denied)?;
            }
            self.inner.take(minimum, context)
        }
        fn give(&mut self, buffer: TokenBuffer, context: &mut ConversionContext) -> Result<()> {
            self.state.gives.fetch_add(1, Ordering::Relaxed);
            if self.state.stage.compare_exchange(2, 0, Ordering::Relaxed, Ordering::Relaxed).is_ok() {
                let denied = context.allocation_remaining().checked_add(1).unwrap();
                context.reserve(denied)?;
            }
            self.inner.give(buffer, context)
        }
        fn close(&mut self, context: &mut ConversionContext) -> Result<()> {
            self.state.closes.fetch_add(1, Ordering::Relaxed);
            self.inner.close(context)
        }
        fn abandon(&mut self) {
            self.state.abandons.fetch_add(1, Ordering::Relaxed);
            self.inner.abandon();
        }
    }
    for stage in [1, 2] {
        let budget = MemoryBudget::new(usize::MAX);
        let mut context = ConversionContext::new(Arc::clone(&budget));
        let charge = context.reserve(4096).unwrap();
        let state = Arc::new(FailureState { stage: AtomicUsize::new(stage), acquires: AtomicUsize::new(0), gives: AtomicUsize::new(0), closes: AtomicUsize::new(0), abandons: AtomicUsize::new(0), charge });
        let pool = pool::ThreadTokenPool::new(Profile::Jackson221, &mut context).unwrap();
        let intern = Intern::new(&mut context);
        let buffers = FailOnce { inner: pool.buffers(), state: Arc::clone(&state) };
        let mut runtime = AvroSchemaRuntime::new(factory(&mut context), buffers, intern.clone(), &mut context).unwrap();
        let failure = runtime.parse(Some(JavaText::from_utf8("\"int\"")), &[], &BTreeMap::new(), policy(), &mut context).err().unwrap();
        assert!(failure.code.ends_with("batch-too-large"));
        assert_eq!(state.acquires.load(Ordering::Relaxed), 1);
        assert_eq!(state.gives.load(Ordering::Relaxed), usize::from(stage == 2));
        assert_eq!(state.closes.load(Ordering::Relaxed), 0);
        assert_eq!(state.abandons.load(Ordering::Relaxed), 1);
        drop(context);
        let mut next = ConversionContext::new(Arc::clone(&budget));
        let datum = runtime.parse(Some(JavaText::from_utf8("\"int\"")), &[], &BTreeMap::new(), policy(), &mut next).unwrap().unwrap();
        assert_eq!(state.acquires.load(Ordering::Relaxed), 2);
        assert_eq!(state.closes.load(Ordering::Relaxed), 1);
        assert_eq!(state.abandons.load(Ordering::Relaxed), 1);
        drop(datum);
        drop(runtime);
        drop(intern);
        drop(pool);
        drop(state);
        drop(next);
        assert_eq!(budget.used_bytes(), 0);
    }
}
