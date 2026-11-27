use std::{future::Future, sync::{Arc, atomic::{AtomicBool, Ordering}}};
use parking_lot::{Mutex, RwLock, RwLockReadGuard};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_node::TransactionClock;
use rtdb_server::{ServerDriver, ServerHandle, config::{ParsedConfiguration, ProcessEnvironment}};
use rtdb_types::{Anomaly, Result};
use tokio::{runtime::Runtime, task::JoinHandle};

struct Admission { closing: AtomicBool, calls: RwLock<()> }

pub(crate) struct EmbeddedRegion {
    pub server: ServerHandle,
    runtime: Runtime,
    driver: Mutex<Option<JoinHandle<Result<()>>>>,
    admission: Admission,
    _reservation: Arc<MemoryReservation>,
    _configuration: Arc<MemoryReservation>,
}

impl EmbeddedRegion {
    pub fn open(yaml: &str, memory_bytes: usize, properties: &str, transaction_clock: Option<Arc<TransactionClock>>) -> Result<Arc<Self>> {
        blocking_caller()?;
        if memory_bytes == 0 { return Err(super::invalid("A positive allocation limit is required")); }
        let bootstrap = MemoryBudget::new(memory_bytes);
        let object_sort = rtdb_server::config::object_sort_properties(properties,&bootstrap)?;
        let parsed = ParsedConfiguration::from_yaml(yaml, &ProcessEnvironment, &bootstrap, false)?.with_object_sort(object_sort);
        let (configuration, memory, configuration_charge) = parsed.into_execution(memory_bytes)?;
        drop(bootstrap);
        let runtime = tokio::runtime::Builder::new_multi_thread().thread_stack_size(rtdb_node::WORKER_STACK_BYTES).enable_all().build().map_err(|error| Anomaly::fault("rtdb.ffi/runtime", error.to_string()))?;
        let configuration = Box::new(configuration);
        let (server, driver) = Self::run_on(&runtime, move || async move {
            match transaction_clock {
                Some(clock) => ServerDriver::open_with_log_clock(*configuration, memory_bytes, memory, clock).await,
                None => ServerDriver::open_with_memory(*configuration, memory_bytes, memory).await,
            }
        })?;
        let reservation = match server.node().memory().reserve(std::mem::size_of::<Self>() + 8192) {
            Ok(reservation) => reservation,
            Err(error) => { server.shutdown(); let _ = Self::run_on(&runtime, move || driver.run()); return Err(error); }
        };
        let driver = runtime.spawn(async move { Box::pin(driver.run()).await });
        Ok(Arc::new(Self { server, runtime, driver: Mutex::new(Some(driver)), admission: Admission { closing: AtomicBool::new(false), calls: RwLock::new(()) }, _reservation: reservation, _configuration: configuration_charge }))
    }
    pub fn memory(&self) -> &Arc<MemoryBudget> { self.server.node().memory() }
    pub fn enter(&self) -> Result<RwLockReadGuard<'_, ()>> {
        let guard = self.admission.calls.read();
        if self.admission.closing.load(Ordering::Acquire) { return Err(super::closed()); }
        Ok(guard)
    }
    /// Runs an async operation on a Tokio worker thread (`rtdb_node::WORKER_STACK_BYTES`
    /// stack) so large debug futures and deep planner/query recursion never overflow a
    /// host thread's smaller stack or run JNI/FFM callbacks on a switched `stacker` stack.
    pub fn run<F, Fut, T>(&self, work: F) -> Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        Self::run_on(&self.runtime, work)
    }
    fn run_on<F, Fut, T>(runtime: &Runtime, work: F) -> Result<T>
    where
        F: FnOnce() -> Fut + Send + 'static,
        Fut: Future<Output = Result<T>> + Send + 'static,
        T: Send + 'static,
    {
        blocking_caller()?;
        let task = runtime.spawn(async move { Box::pin(work()).await });
        runtime.block_on(task).map_err(|error| {
            if error.is_panic() { Anomaly::fault("rtdb.ffi/panic", "A native call panicked") }
            else { Anomaly::fault("rtdb.ffi/task", error.to_string()) }
        })?
    }
    pub fn close(&self) -> Result<()> {
        blocking_caller()?;
        self.admission.closing.store(true, Ordering::Release);
        self.server.shutdown();
        self.server.node().shutdown();
        let _drained = self.admission.calls.write();
        if let Some(driver) = self.driver.lock().take() {
            self.run(move || async move { driver.await.map_err(|error| Anomaly::fault("rtdb.ffi/driver", error.to_string()))? })?;
        }
        Ok(())
    }
}
impl Drop for EmbeddedRegion { fn drop(&mut self) { let _ = self.close(); } }

pub(crate) struct DatabaseHandle(pub Arc<EmbeddedRegion>);
impl Drop for DatabaseHandle { fn drop(&mut self) { let _ = self.0.close(); } }
pub(crate) fn blocking_caller() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() { return Err(super::invalid("Blocking FFI calls must originate outside a Tokio runtime")); }
    Ok(())
}
