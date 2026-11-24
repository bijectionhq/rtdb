# Native Multi follower integration

The opt-in listener serves one existing local replica of a ready plain Multi deployment.
The cell is responsible for selecting that replica, owning the current assignment lease, and delivering the per-assignment credentials and TLS trust to the worker.

## Installed process

Start the official Multi binary with `rtdb node --multi --config-env CELL_RTDB_YAML --native-follower /run/private/rtdb/follower.json`.
The ordinary `-f FILE` configuration source also works.
The follower reuses the installed Multi journal, storage and key-vault settings and reads the ordered catalog to find the exact ready incarnation.
Prepare the catalog and data journals, the deployment's incarnation marker, and the keys before launch.
Run this process separately from the writer and omit its node address from the writers' `RTDB_PEERS` discovery records.
The process attaches only the assigned replica, replays its journal and binds only the private follower Flight listener.
A missing or incompatible prepared replica fails startup.
The process never enters Multi election, recovery, peer forwarding, or automatic attachment, including during shutdown.

Use an owner-only launch file and input files under a cell-private directory.
The certificate chain and private key are PEM files.
The password file contains exactly 64 random hexadecimal digits without a newline.
Use a dedicated Basic user and a certificate valid for the server name the cell gives the worker.

```json
{
  "slot": "pool-a",
  "assignment": {
    "database": "app",
    "incarnation": "10000000-0000-0000-0000-000000000002",
    "placement_epoch": 3,
    "assignment": "10000000-0000-0000-0000-000000000003",
    "endpoint": "127.0.0.1:0"
  },
  "certificate": "/run/private/rtdb/tls.pem",
  "private_key": "/run/private/rtdb/key.pem",
  "user": "pool-reader",
  "password_file": "/run/private/rtdb/password"
}
```

The cell holds the child's stdin and stdout pipes open.
The first stdout line is JSON containing `slot` and the full `assignment`, with the actual bound loopback endpoint replacing port zero.
The cell uses that exact assignment for `LocalPlacement` resolve and attach on its private socket.
Its attach reply supplies the same assignment, `tls_server_name`, `ca_pem`, `user`, and `password`.
Use an authenticated progress probe before admitting work to that route.

On retirement, the cell stops renewing the assignment and drains the Bijection attachment.
For `POST /v1/follower/detach`, the cell verifies the current full assignment and sends the same `{"slot":...,"assignment":...}` body as one newline-terminated JSON frame on stdin.
A mismatched assignment frame returns `{"error":"rtdb/follower-assignment"}` and leaves the listener running.
The cell maps that refusal to HTTP 409.
An exact frame shuts down and joins Flight, the local node and its resources before writing `{"assignment":...}` to stdout and exiting successfully.
The cell waits for that exact receipt and successful child exit before acknowledging detach or removing the local copy.
It retains the exact retired assignment for idempotent detach retries after a lost response.
A signal, lost control pipe or process failure yields no detach receipt.
The cell owns the private HTTP adapter, credentials delivered to the worker, and receipt retention.

## Embedded host API

The cell host calls the following API after the existing Multi runtime has attached the assigned database locally.
Use an existing replica on a separate member from the writer.
The host can bind before driving that member's election loop when the exact attachment already exists.

```rust
// rtdb_server::multi, feature `multi`
pub struct NativeFollowerAssignment {
    pub database: String,
    pub incarnation: uuid::Uuid,
    pub placement_epoch: u64,
    pub assignment: uuid::Uuid,
}

impl MultiRuntime {
    pub async fn bind_native_follower(
        &self,
        node: &rtdb_node::Node,
        assignment: NativeFollowerAssignment,
        address: std::net::SocketAddr,
        authenticator: std::sync::Arc<rtdb_auth::Authenticator>,
        tls: tokio_rustls::rustls::ServerConfig,
    ) -> rtdb_types::Result<(rtdb_flight_sql::ServerHandle, rtdb_flight_sql::ServerDriver)>;
}
```

Supply a loopback bind address, optionally with port `0`, nonzero assignment and incarnation UUIDs, and a positive placement epoch.
Use an authenticator dedicated to the assignment with a verified password credential that can read the selected database.
Use a certificate that the worker can verify for its configured server name.
RTDB verifies the ordered deployment registration, exact local attachment, and follower eligibility before returning the bound listener.
An active writer, a resident writer candidate, or a second assignment for the same local replica refuses the bind.
While the listener owns the replica, the Multi driver replays authenticated records and skips election and writer recovery for that replica.

Run `ServerDriver::run(self)` and publish `ServerHandle::address()` as `FollowerAssignment.endpoint` only for that assignment.
The bound address identifies the transport, while a successful authenticated progress probe establishes readiness.
The worker can pass that endpoint, the TLS trust and the Basic credential to `native_store_runtime::rtdb::FollowerRoute::plain_multi`.
Send exactly one `x-xtdb-database` header on each call with the value `NAME@INCARNATION`, where the incarnation is 32 lowercase hex digits without hyphens.
The listener admits authenticated Flight handshake, `rtdb.v1.Capabilities` and Query DoGet.
Capabilities advertises `query`, `query.track`, `query.page`, `query.applied-position` and `query.follower-progress`.
Transact, including effect-free validation, TxStatus, Changes, ordinary Flight SQL, administration, custody operations and other Flight actions are refused at dispatch.
Forwarded peer and audience headers cannot admit a call on this listener, and the legacy fresh-replica proxy keeps its existing Query refusal.
Query uses one database snapshot and read-only SQL, so SQL mutations, DDL and session commands are also refused.

The cell stops renewing the assignment, drains the Bijection attachment, calls `ServerHandle::shutdown()` and joins the Flight driver.
Stopping this listener releases only its local replica reservation.
The cell owns retirement of the assigned member's local copy and must compare the full assignment before stopping or removing it.
This API mounts on the managed plain Multi runtime in `rtdb-server` and leaves task provisioning and a concrete `FollowerPlacement` adapter to the cell.

## Query and progress

Use `Source { branch: "main", layer: MERGED, at: Latest | AtLeast(basis) | Exactly(basis) }`.
Latest and AtLeast capture the current locally applied cut.
An ahead request can be retried as apply progresses.
Exactly echoes the retained cut byte-for-byte, including on an empty or continued page.
Tracking and pagination use the existing native token authority for the same incarnation.
Receiver clock and protected release evidence require their owning routes.

For `FollowerPlacement::watermarks`, issue `SELECT 1 LIMIT 0` at `Exactly(actual_basis)` with `QueryRequest.observe_applied_position = true` (tag 9) over the same private TLS route.
Check that `QueryTrailer.basis` (tag 1) exactly equals `actual_basis` and `QueryTrailer.applied_position` (tag 8) is present.
Require `QueryTrailer.follower_progress` (tag 9) and compare the database, incarnation, positive placement epoch, assignment ID and endpoint to the installed assignment.
`FollowerProgress.writer_head` (tag 6) is the opaque Basis of the settled writer head and `FollowerProgress.writer_head_position` (tag 7) is its comparable position.
Return the selected Basis and position as `applied`, and the two writer-head fields as `writer_head`.
Both positions come from the incarnation's recorded transaction cuts and may skip journal positions used for controls or unsettled transactions.
The writer head is sampled from the authenticated committed journal prefix after the replica takes its local selection, with a live writer lease in that prefix.
Subtract positions only within the verified assignment's incarnation and keep both Basis values opaque.
The progress reply is authenticated by the live TLS connection to that assignment's endpoint.

| Condition | Flight result |
| --- | --- |
| Exactly or AtLeast names a cut ahead of local apply | `rtdb/basis-ahead` |
| Exact cut no longer readable or no retained cut at that position | `rtdb/cursor-expired` or `rtdb/erased-under-view` |
| Basis belongs to another incarnation | `rtdb/foreign-basis` |
| Selector differs from the installed assignment | `rtdb/follower-assignment` |
| Exact local attachment has detached | `rtdb/follower-unavailable` |
| Writer head cannot cover the selected cut or its current lease is absent | `rtdb/follower-progress-unavailable` |
| Source or authority option is unavailable | `rtdb/unsupported` with feature `query.follower-source` or `query.follower-authority` |
| RPC outside the allowlist | gRPC `PERMISSION_DENIED` |

The cell maps absent or unavailable paired evidence to `rtdb.follower/progress-unavailable` and mismatched evidence to the corresponding Bijection assignment or invalid-progress error.
The additive trailer field requires the producer IDL when implementing that provider.
The existing Bijection facade can already attach, query and drain against the published route and delegates pairing the progress evidence to the provider.

## Qualification

The source fixtures cover local lag, settled writer progress across journal controls, election exclusion, exact retention and the Flight call allowlist.
Joint runtime qualification needs two Multi members and the cell adapter, including wrong assignment and credentials, proxy-only refusal, empty-effect Transact refusal, tracked empty and paged reads, writer failover and drain with an admitted exact read.
