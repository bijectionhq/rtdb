use std::{
    collections::{BTreeSet, VecDeque},
    net::IpAddr,
    sync::Arc,
};

use bytes::Bytes;
use journal_contract::{JournalError, Lsn};
use rtdb_format::journal::Record;
use rtdb_tx::transact::{
    Decision, LINEARIZABLE, Receipt, TIMED_TRANSACT, Transact, TransactPipeline, TransactionRecord,
};
use rtdb_types::{Anomaly, AnomalyCategory, Result, Timestamp};
use tokio::{
    sync::{Mutex, mpsc, oneshot},
    task::JoinHandle,
};

use super::{
    Entry, Lease, MultiDatabase, Role, Stream, missing_receipt, next, no_writer, reject_erasure,
};

pub(super) const CAPACITY: usize = 64;
const FEATURES: &[&str] = &[LINEARIZABLE, TIMED_TRANSACT];
type Reply<T> = oneshot::Sender<Result<T>>;

enum Command {
    Transact(Transact, Reply<Receipt>),
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

struct Pending {
    request: Transact,
    reply: Reply<Receipt>,
}

struct Staged {
    key: Vec<u8>,
    reply: Reply<Receipt>,
    write: Append,
}

pub(super) struct Append {
    pub(super) at: Lsn,
    pub(super) entry: Entry,
}

pub(super) struct Authority {
    stream: Stream,
    clock: Arc<dyn Fn() -> Timestamp + Send + Sync>,
    drift_micros: i64,
    pub(super) lease: Lease,
}

impl Authority {
    pub(super) async fn append(&self, write: &Append) -> std::result::Result<(), AttemptError> {
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

pub(super) enum AttemptError {
    Local(Anomaly),
    Journal(JournalError),
}

async fn run(database: Arc<Mutex<MultiDatabase>>, mut input: mpsc::Receiver<Command>) {
    let mut barrier = None;
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
                    Pending { request, reply },
                )
                .await;
            }
            Command::Transact(request, reply) => {
                let result = database.lock().await.transact(&request).await;
                let _ = reply.send(result);
            }
            Command::Tick(live, reply) => {
                let result = database.lock().await.tick(&live).await;
                let _ = reply.send(result);
            }
        }
    }
}

async fn burst(
    database: &Mutex<MultiDatabase>,
    input: &mut mpsc::Receiver<Command>,
    barrier: &mut Option<Command>,
    first: Pending,
) {
    let mut queued = vec![first];
    fill(input, barrier, &mut queued, CAPACITY);
    let mut staged = VecDeque::new();
    stage_batch(database, &mut staged, queued).await;
    let mut input_open = true;
    while let Some(Staged { key, reply, write }) = staged.pop_front() {
        let authority = match database.lock().await.append_authority(write.entry.epoch) {
            Ok(authority) => authority,
            Err(error) => {
                let _ = reply.send(Err(error));
                withdraw(database, staged).await;
                return;
            }
        };
        let result = {
            let append = authority.append(&write);
            tokio::pin!(append);
            loop {
                tokio::select! {
                    result = &mut append => break result,
                    command = input.recv(), if input_open && barrier.is_none() && staged.len() + 1 < CAPACITY => {
                        match command {
                            Some(Command::Transact(request, reply)) if ordinary(&request) => {
                                let mut group = vec![Pending { request, reply }];
                                fill(input, barrier, &mut group, CAPACITY - staged.len() - 1);
                                stage_batch(database, &mut staged, group).await;
                            }
                            Some(command) => *barrier = Some(command),
                            None => input_open = false,
                        }
                    }
                }
            }
        };
        let receipt = database
            .lock()
            .await
            .publish_staged(authority.lease, write, result)
            .await
            .and_then(|receipts| receipt_for(receipts, &key));
        match receipt {
            Ok(receipt) => {
                let _ = reply.send(Ok(receipt));
            }
            Err(error) => {
                let _ = reply.send(Err(error));
                withdraw(database, staged).await;
                return;
            }
        }
    }
}

fn fill(
    input: &mut mpsc::Receiver<Command>,
    barrier: &mut Option<Command>,
    queued: &mut Vec<Pending>,
    limit: usize,
) {
    while barrier.is_none() && queued.len() < limit {
        match input.try_recv() {
            Ok(Command::Transact(request, reply)) if ordinary(&request) => {
                queued.push(Pending { request, reply })
            }
            Ok(command) => *barrier = Some(command),
            Err(_) => break,
        }
    }
}

async fn stage_batch(
    database: &Mutex<MultiDatabase>,
    staged: &mut VecDeque<Staged>,
    queued: Vec<Pending>,
) {
    let mut requests = Vec::with_capacity(queued.len());
    let mut replies = Vec::with_capacity(queued.len());
    for Pending { request, reply } in queued {
        match reject_erasure(&request) {
            Ok(()) => {
                replies.push((request.key.clone(), reply));
                requests.push(request);
            }
            Err(error) => {
                let _ = reply.send(Err(error));
            }
        }
    }
    let decided = database.lock().await.decide_group(&requests);
    match decided {
        Ok(decisions) => {
            for ((key, reply), decision) in replies.into_iter().zip(decisions) {
                match decision {
                    Ok(Planned::Append(write)) => staged.push_back(Staged { key, reply, write }),
                    Ok(Planned::Answer(receipt)) => {
                        let _ = reply.send(Ok(receipt));
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error));
                    }
                }
            }
        }
        Err(error) => {
            for (_, reply) in replies {
                let _ = reply.send(Err(error.clone()));
            }
        }
    }
}

async fn withdraw(database: &Mutex<MultiDatabase>, staged: VecDeque<Staged>) {
    database.lock().await.withdraw_writer();
    for Staged { reply, .. } in staged {
        let _ = reply.send(Err(Anomaly::new(
            AnomalyCategory::Interrupted,
            "rtdb/speculative-write-withdrawn",
            "An earlier append failed before this write was issued",
        )));
    }
}

pub(super) enum Planned {
    Answer(Receipt),
    Append(Append),
}

impl MultiDatabase {
    pub(super) fn new_pipeline(&self) -> Result<TransactPipeline> {
        let state = self.state();
        let durable = &state.transactions;
        let snapshot = durable.encode_checkpoint()?;
        let copy =
            rtdb_tx::transact::TransactState::decode_checkpoint(&snapshot, durable.index().head())?;
        Ok(TransactPipeline::new(copy, next(state.head)?.0, CAPACITY))
    }

    pub(super) fn decide_one(&mut self, request: &Transact) -> Result<Planned> {
        let now = (self.clock)();
        if request.ops.is_empty() && request.otherwise.is_empty() {
            let validation = match self.writer.as_mut() {
                Some(writer) => writer
                    .pipeline
                    .decide(request, now, FEATURES, &self.claims)?,
                None => self
                    .new_pipeline()?
                    .decide(request, now, FEATURES, &self.claims)?,
            };
            return match validation {
                Decision::Answer(receipt) => Ok(Planned::Answer(receipt)),
                Decision::Append { .. } => Err(Anomaly::fault(
                    "rtdb/validation-append",
                    "Validation allocated a journal record",
                )),
            };
        }
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let generation = self.state().generation;
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        let decision = writer
            .pipeline
            .decide(request, now, FEATURES, &self.claims)?;
        plan(decision, lease, generation)
    }

    fn decide_group(&mut self, requests: &[Transact]) -> Result<Vec<Result<Planned>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let lease = self.authority()?;
        self.require_armed_rotation()?;
        let generation = self.state().generation;
        let now = (self.clock)();
        let writer = self.writer.as_mut().ok_or_else(no_writer)?;
        Ok(writer
            .pipeline
            .decide_batch(requests, now, FEATURES, &self.claims)
            .into_iter()
            .map(|decision| decision.and_then(|decision| plan(decision, lease, generation)))
            .collect())
    }

    pub(super) fn decide_terminal(&mut self, lease: Lease) -> Result<Append> {
        let upper = Timestamp(self.upper());
        let generation = self.state().generation;
        let decision = self
            .writer
            .as_mut()
            .ok_or_else(no_writer)?
            .pipeline
            .terminal(upper)?;
        match plan(decision, lease, generation)? {
            Planned::Append(write) => Ok(write),
            Planned::Answer(_) => Err(missing_receipt()),
        }
    }

    pub(super) fn append_authority(&self, epoch: journal_contract::Epoch) -> Result<Authority> {
        let lease = self.authority()?;
        if lease.epoch != epoch {
            return Err(no_writer());
        }
        Ok(Authority {
            stream: self.stream.clone(),
            clock: Arc::clone(&self.clock),
            drift_micros: self.options.drift_micros,
            lease,
        })
    }

    pub(super) async fn publish_staged(
        &mut self,
        lease: Lease,
        write: Append,
        result: std::result::Result<(), AttemptError>,
    ) -> Result<Vec<Receipt>> {
        match result {
            Ok(()) => {}
            Err(AttemptError::Journal(error)) => {
                let error = self.mutation_failed(error);
                self.withdraw_writer();
                return Err(interrupted(error));
            }
            Err(AttemptError::Local(error)) => {
                self.withdraw_writer();
                return Err(interrupted(error));
            }
        }
        let Append { at, entry } = write;
        match self.committed.as_mut() {
            Some(state) if next(state.head)? == at => {
                if let Err(error) = state.apply(at, entry) {
                    self.committed = None;
                    self.withdraw_writer();
                    return Err(interrupted(error));
                }
            }
            Some(state) if state.head >= at => {}
            _ => self.refresh().await.map_err(interrupted)?,
        }
        self.require_authority(lease).map_err(interrupted)?;
        let writer = self
            .writer
            .as_mut()
            .filter(|writer| writer.lease.epoch == lease.epoch)
            .ok_or_else(|| interrupted(no_writer()))?;
        Ok(writer.pipeline.acknowledge(at.0))
    }

    pub(super) fn withdraw_writer(&mut self) {
        let head = self.committed.as_ref().map_or(Lsn(0), |state| state.head);
        if let Some(mut writer) = self.writer.take() {
            writer.pipeline.withdraw(head.0.saturating_add(1));
        }
    }
}

fn plan(decision: Decision, lease: Lease, generation: u64) -> Result<Planned> {
    Ok(match decision {
        Decision::Answer(receipt) => Planned::Answer(receipt),
        Decision::Append { position, record } => Planned::Append(Append {
            at: Lsn(position),
            entry: Entry {
                epoch: lease.epoch,
                generation,
                record: journal_record(record)?,
            },
        }),
    })
}

fn journal_record(payload: Bytes) -> Result<Record> {
    Ok(match TransactionRecord::decode(&payload)? {
        TransactionRecord::Tx(_) => Record::Tx {
            payload: payload.to_vec(),
            objects: Vec::new(),
        },
        TransactionRecord::Settle { tx_lsn } => Record::Settle { tx_lsn },
        TransactionRecord::Refuse { tx_lsn } => Record::Refuse { tx_lsn },
    })
}

pub(super) fn receipt_for(receipts: Vec<Receipt>, key: &[u8]) -> Result<Receipt> {
    receipts
        .into_iter()
        .find(|receipt| receipt.key == key)
        .ok_or_else(missing_receipt)
}

pub(super) fn interrupted(error: Anomaly) -> Anomaly {
    Anomaly::new(
        AnomalyCategory::Interrupted,
        error.code,
        "The write outcome is unknown. Recover it with TxStatus",
    )
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
