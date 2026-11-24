use std::{
    collections::{BTreeSet, VecDeque},
    net::IpAddr,
    sync::Arc,
};

use journal_contract::{JournalError, Lsn};
use rtdb_format::journal::Record;
use rtdb_tx::transact::{
    LINEARIZABLE, Receipt, Resolution, ScopedTransact, TIMED_TRANSACT, Transact, TransactState,
};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp, rtdb_v1::{ClaimSealer, LabelScope}};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};

use super::{
    Entry, Lease, MultiDatabase, Role, Stream, missing_receipt, next, no_writer, reject_erasure,
};

const CAPACITY: usize = 64;
type Reply<T> = oneshot::Sender<Result<T>>;

enum Command {
    Transact(Transact, Reply<Receipt>),
    ScopedTransact(Transact, LabelScope, [u8; 32], Reply<Receipt>),
    Tick(BTreeSet<IpAddr>, Reply<Role>),
}

pub struct WritePipeline {
    input: mpsc::Sender<Command>,
    task: JoinHandle<()>,
}

impl WritePipeline {
    pub fn start(database: Arc<Mutex<MultiDatabase>>) -> Self {
        let (input, receive) = mpsc::channel(CAPACITY);
        let task = tokio::spawn(run(database, receive));
        Self { input, task }
    }

    pub async fn transact(&self, request: Transact) -> Result<Receipt> {
        let (reply, result) = oneshot::channel();
        self.input
            .send(Command::Transact(request, reply))
            .await
            .map_err(|_| pipeline_closed())?;
        result.await.map_err(|_| pipeline_closed())?
    }

    pub async fn transact_scoped(&self, request: Transact, labels: LabelScope, receipt_namespace: [u8; 32]) -> Result<Receipt> {
        let (reply, result) = oneshot::channel();
        self.input.send(Command::ScopedTransact(request, labels, receipt_namespace, reply))
            .await.map_err(|_| pipeline_closed())?;
        result.await.map_err(|_| pipeline_closed())?
    }

    pub async fn tick(&self, live: BTreeSet<IpAddr>) -> Result<Role> {
        let (reply, result) = oneshot::channel();
        self.input
            .send(Command::Tick(live, reply))
            .await
            .map_err(|_| pipeline_closed())?;
        result.await.map_err(|_| pipeline_closed())?
    }
}

impl Drop for WritePipeline {
    fn drop(&mut self) {
        self.task.abort();
    }
}

enum Staged {
    Append {
        write: Append,
        reply: Reply<Receipt>,
    },
    Answer {
        result: Result<Receipt>,
        reply: Reply<Receipt>,
    },
}

impl Staged {
    fn reject(self, error: Anomaly) {
        let reply = match self {
            Staged::Append { reply, .. } | Staged::Answer { reply, .. } => reply,
        };
        let _ = reply.send(Err(error));
    }
}

struct Append {
    at: Lsn,
    receipt: Receipt,
    entry: Entry,
}

struct Overlay {
    transactions: TransactState,
    head: Lsn,
    generation: u64,
    claims: Arc<ClaimSealer>,
}

struct Authority {
    stream: Stream,
    clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
    drift_micros: i64,
    lease: Lease,
}

impl Authority {
    async fn append(&self, write: &Append) -> std::result::Result<(), AttemptError> {
        let bytes = self
            .stream
            .encode(write.at, &write.entry)
            .await
            .map_err(AttemptError::Local)?;
        if (self.clock)().0.saturating_add(self.drift_micros) >= self.lease.until {
            return Err(AttemptError::Local(no_writer()));
        }
        let appended = self
            .stream
            .append(self.lease.epoch, write.at, bytes)
            .await
            .map_err(AttemptError::Journal)?;
        if appended != write.at {
            return Err(AttemptError::Local(Anomaly::fault(
                "rtdb/journal-position",
                "The append acknowledged a different position",
            )));
        }
        Ok(())
    }
}

enum AttemptError {
    Local(Anomaly),
    Journal(JournalError),
}

async fn run(database: Arc<Mutex<MultiDatabase>>, mut input: mpsc::Receiver<Command>) {
    let mut barrier = None;
    let mut cached_overlay = None;
    loop {
        let command = match barrier.take() {
            Some(command) => command,
            None => match input.recv().await {
                Some(command) => command,
                None => return,
            },
        };
        match command {
            Command::Transact(request, reply) if ordinary(&request) => {
                burst(
                    &database,
                    &mut input,
                    &mut barrier,
                    &mut cached_overlay,
                    request,
                    reply,
                )
                .await;
            }
            Command::Transact(request, reply) => {
                let mut database = database.lock().await;
                let result = database.transact(&request).await;
                if database.role() == Role::Follower {
                    cached_overlay = None;
                }
                let _ = reply.send(result);
            }
            Command::ScopedTransact(request, labels, receipt_namespace, reply) => {
                let mut database = database.lock().await;
                let scope = ScopedTransact { labels: &labels, receipt_namespace };
                let result = database.transact_scoped(&request, scope).await;
                cached_overlay = None;
                let _ = reply.send(result);
            }
            Command::Tick(live, reply) => {
                let result = database.lock().await.tick(&live).await;
                if !matches!(result, Ok(Role::Writer(_))) {
                    cached_overlay = None;
                }
                let _ = reply.send(result);
            }
        }
    }
}

async fn burst(
    database: &Mutex<MultiDatabase>,
    input: &mut mpsc::Receiver<Command>,
    barrier: &mut Option<Command>,
    cached_overlay: &mut Option<(Overlay, Authority)>,
    first: Transact,
    reply: Reply<Receipt>,
) {
    let prepared = database
        .lock()
        .await
        .start_overlay(cached_overlay.take())
        .await;
    let (mut overlay, authority) = match prepared {
        Ok(values) => values,
        Err(error) => {
            let _ = reply.send(Err(error));
            return;
        }
    };
    let mut staged = VecDeque::from([overlay.stage(&authority, first, reply)]);
    let mut input_open = true;
    loop {
        while barrier.is_none() && staged.len() < CAPACITY {
            let Ok(command) = input.try_recv() else { break };
            overlay.admit(&authority, command, &mut staged, barrier);
        }
        let Some(next) = staged.pop_front() else {
            *cached_overlay = Some((overlay, authority));
            return;
        };
        let (write, reply) = match next {
            Staged::Answer { result, reply } => {
                let _ = reply.send(result);
                continue;
            }
            Staged::Append { write, reply } => (write, reply),
        };
        let result = {
            let append = authority.append(&write);
            tokio::pin!(append);
            loop {
                tokio::select! {
                    result = &mut append => break result,
                    command = input.recv(), if input_open && barrier.is_none() && staged.len() < CAPACITY => {
                        if let Some(command) = command {
                            overlay.admit(&authority, command, &mut staged, barrier);
                        } else {
                            input_open = false;
                        }
                    }
                }
            }
        };
        let result = database
            .lock()
            .await
            .publish_staged(authority.lease, write, result)
            .await;
        match result {
            Ok(receipt) => {
                let _ = reply.send(Ok(receipt));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                for request in staged {
                    request.reject(Anomaly::new(
                        AnomalyCategory::Unavailable,
                        "rtdb/speculative-write-withdrawn",
                        "An earlier append failed before this write was issued",
                    ));
                }
                return;
            }
        }
    }
}

impl Overlay {
    fn admit(
        &mut self,
        authority: &Authority,
        command: Command,
        staged: &mut VecDeque<Staged>,
        barrier: &mut Option<Command>,
    ) {
        match command {
            Command::Transact(request, reply) if ordinary(&request) => {
                staged.push_back(self.stage(authority, request, reply));
            }
            command => *barrier = Some(command),
        }
    }

    fn stage(&mut self, authority: &Authority, request: Transact, reply: Reply<Receipt>) -> Staged {
        match self.prepare(authority, &request) {
            Ok(Ok(write)) => Staged::Append { write, reply },
            Ok(Err(answer)) => Staged::Answer {
                result: Ok(answer),
                reply,
            },
            Err(error) => Staged::Answer {
                result: Err(error),
                reply,
            },
        }
    }

    fn prepare(
        &mut self,
        authority: &Authority,
        request: &Transact,
    ) -> Result<std::result::Result<Append, Receipt>> {
        reject_erasure(request)?;
        let at = next(self.head)?;
        let resolution = self.transactions.resolve(
            request,
            at.0,
            (authority.clock)(),
            &[LINEARIZABLE, TIMED_TRANSACT, "row-visibility"],
            &self.claims,
        )?;
        let prepared = match resolution {
            Resolution::Answer(answer) => return Ok(Err(answer)),
            Resolution::Append(prepared) => prepared,
        };
        let record = prepared.into_record();
        let payload = record.encode()?;
        let receipt = self
            .transactions
            .apply(at.0, record)?
            .ok_or_else(missing_receipt)?;
        self.head = at;
        let entry = Entry {
            epoch: authority.lease.epoch,
            generation: self.generation,
            record: Record::Tx {
                payload: payload.to_vec(),
                objects: Vec::new(),
            },
        };
        Ok(Ok(Append { at, receipt, entry }))
    }
}

impl MultiDatabase {
    async fn start_overlay(
        &mut self,
        cached: Option<(Overlay, Authority)>,
    ) -> Result<(Overlay, Authority)> {
        if self.committed.is_none() {
            self.refresh().await?;
        }
        let lease = self.authority()?;
        self.refuse_pending(lease).await?;
        let state = self.state();
        if let Some((overlay, authority)) = cached
            && overlay.head == state.head
            && overlay.generation == state.generation
            && authority.lease.epoch == lease.epoch
            && authority.lease.until == lease.until
        {
            return Ok((overlay, authority));
        }
        Ok((
            Overlay {
                transactions: state.transactions.clone(),
                head: state.head,
                generation: state.generation,
                claims: Arc::clone(&self.claims),
            },
            Authority {
                stream: self.stream.clone(),
                clock: Arc::clone(&self.clock),
                drift_micros: self.options.drift_micros,
                lease,
            },
        ))
    }

    async fn publish_staged(
        &mut self,
        lease: Lease,
        write: Append,
        result: std::result::Result<(), AttemptError>,
    ) -> Result<Receipt> {
        match result {
            Ok(()) => {}
            Err(AttemptError::Journal(error)) => return Err(self.mutation_failed(error)),
            Err(AttemptError::Local(error)) => {
                self.writer = None;
                return Err(error);
            }
        }
        let Append { at, receipt, entry } = write;
        let previous = Lsn(at.0 - 1);
        let landed = match self.committed.as_mut() {
            Some(state) if state.head == previous => match state
                .apply(at, entry)
                .and_then(|receipt| receipt.ok_or_else(missing_receipt))
            {
                Ok(receipt) => receipt,
                Err(error) => {
                    self.committed = None;
                    self.writer = None;
                    return Err(error);
                }
            },
            Some(state) if state.head >= at => Receipt {
                replayed: false,
                ..state.transactions.tx_status(&receipt.key)?
            },
            _ => {
                self.refresh().await?;
                Receipt {
                    replayed: false,
                    ..self.state().transactions.tx_status(&receipt.key)?
                }
            }
        };
        if landed != receipt {
            self.committed = None;
            self.writer = None;
            return Err(Anomaly::fault(
                "rtdb/journal-receipt",
                "The committed receipt differs from the resolved write",
            ));
        }
        self.require_authority(lease)?;
        if self.authority()?.epoch != lease.epoch {
            return Err(no_writer());
        }
        Ok(receipt)
    }
}

fn ordinary(request: &Transact) -> bool {
    request.window.before.is_none() && (!request.ops.is_empty() || !request.otherwise.is_empty())
}

fn pipeline_closed() -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Unavailable,
        "rtdb/write-pipeline-closed",
        "The database write pipeline is closed",
    )
}
