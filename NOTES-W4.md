# W4 handoff

## Wire and verification

- `rust/proto/rtdb.v1.proto` and `rust/proto/rtdb.admin.v1.proto` define the Flight envelopes and generate Prost types in `rtdb-flight-sql`.
- Flight serves `Capabilities`, main-branch merged `Query`, and a bounded system-time `Changes` feed.
- Query supports positional Arrow IPC parameters, a database-wide authenticated `Claim`, and an authenticated continuation containing the cut, clock, offset and request fingerprint.
- The plan cache reuses the engine's prepared-query catalog validation.
- `RTDB_SEARCH(table, column, query)` and `RTDB_NEAREST(table, column, vector, limit)` expose ranked text and vector reads through SQL table functions.
- `FOR SYSTEM_TIME` and `FOR ALL SYSTEM_TIME` expose history.
- `Transact` and `TxStatus` validate request envelopes and return typed `unsupported` errors pending W5.
- The admin service has generated message types and returns typed `unsupported` until the deployment implementation is connected.
- The executable Flight tests replace the static consumer contract, proof mapping and checker.
- The historical proof results remain in `verification/results/bijection-consumer-v1.md` and identify the revision containing their inputs.
- The point/range benchmark is an ignored integration test and compares wire and held-action round trips on equivalent logical data.

## Design choices and Wave 2 integration

- Default and placed reads share the SQL and Flight path.
- The implemented linearizable routes use a local durable-source barrier or an installed replica read index.
- Shared Kafka and external-source attachments without a read index omit that feature.
- The Flight tests use a local source and a placed fixture.
- Production Kafka and Logkeeper deployments require a mode integration run.
- The current basis and token MAC are bound to a process and database attachment.
- Wave 2 needs a durable database incarnation and a deployment-wide token authority before continuations can survive route changes or restart.
- The ordered `Basis` bytes currently use an order-preserving encoding of the system-time cut.
- `Claim` currently covers `main` and the entire database.
- W5 should validate its MAC using the issuing authority and compare its cut to the committed state before evaluating `Unchanged`.
- Wire SQL paging reexecutes at the original cut and walks the ordered result to the authenticated offset.
- Clients must supply a deterministic total order for paged queries.
- Query exact reopening uses the existing system-time history over a fresh catalog.
- The storage error `rtdb/erased-under-view` maps to wire `not-found`.
- Durable generation-aware token invalidation and historical catalog reconstruction depend on the Wave 2 read source.
- The change feed reads state transitions from SQL temporal history and uses the adopted checkpoint as a conservative coverage floor.
- An update can produce one closed and one opened history rectangle within one system-time group.
- Wave 2 needs to replace this implementation with a feed over the settled applied effects and actual retained floor, including erasure and other transitions without surviving history rectangles.
- The SQL table functions use the existing text-score and vector-distance operators.
- The geographic nearest-neighbour and shape predicates from held spatial actions still need generic SQL geometry support.
- A complete executable migration of the consumer-action traces remains open.
- Snapshot-scoped catalog data can be queried through the wire.
- Node-wide role and metrics providers are outside the scoped wire query context.
- `wire_database` is the branch admission point for W5's branch and layer resolution.
- The wave's branch and draft PR instructions govern the handoff.

## Verification

- `verification/rtdb-v1-conformance` ran the guarded `rtdb test --locked -p rtdb-flight-sql --test flight rtdb_v1` command with the Flight CI stack size.
- Paging and stable errors, SQL parameters and ranking, and the placed replica read-index case passed on `46dd9ff870`.
- The initial Changes case exposed wildcard column ordering and the regression passed after resolving its fields by name.
- The follow-up command was `guarded-cargo.py rtdb test --locked -p rtdb-flight-sql --test flight rtdb_v1_changes`.
- The remote endpoint case and the latency benchmark were compiled as ignored tests.
- The full guarded workspace run was still in progress when the draft was prepared.
- `verification/check_consumer_names.sh` exited 1 for the expected 70 matching lines.
- Shell syntax and diff whitespace checks passed.
- The Allium CLI was unavailable and the new surface was checked against the Allium 3 language reference.
- The bare Cargo run already started before the coordination update was stopped.
- The first guarded runtime attempt needed `RUST_MIN_STACK=16777216`, which the existing Flight CI job uses.

## First-party consumer-name matches

The CI lint checks tracked first-party files and excludes fixture and test paths and vendored dependencies.
Each path and line printed by the lint is listed below.

- `rust/crates/flight-sql/README.adoc:942`
- `rust/crates/flight-sql/README.adoc:1019`
- `rust/crates/flight-sql/README.adoc:1022`
- `rust/crates/flight-sql/src/catalog_actions.rs:413`
- `rust/crates/flight-sql/src/config.rs:150`
- `rust/crates/flight-sql/src/owned_actions.rs:1400`
- `rust/crates/flight-sql/src/owned_actions/commit_tests.rs:87`
- `rust/crates/flight-sql/src/owned_actions/hosted_control.rs:19`
- `rust/crates/flight-sql/src/owned_actions/hosted_control.rs:159`
- `rust/crates/flight-sql/src/owned_actions/hosted_control.rs:240`
- `rust/crates/flight-sql/src/owned_actions/hosted_control.rs:241`
- `rust/crates/multi/src/archive/writable_world/held.rs:2210`
- `rust/crates/multi/src/process/standalone.rs:437`
- `rust/crates/node/src/node.rs:1858`
- `rust/crates/node/src/node/hosted_control.rs:24`
- `rust/crates/node/src/node/hosted_control.rs:25`
- `rust/crates/node/src/node/hosted_control.rs:26`
- `rust/crates/node/src/node/hosted_control.rs:27`
- `rust/crates/node/src/node/hosted_control.rs:28`
- `rust/crates/node/src/node/hosted_control.rs:29`
- `rust/crates/node/src/node/hosted_control.rs:30`
- `rust/crates/query/src/context.rs:92`
- `rust/crates/query/src/hosted_control.rs:28`
- `rust/crates/query/src/hosted_control.rs:44`
- `rust/crates/query/src/scan.rs:4291`
- `rust/crates/server/src/runtime.rs:147`
- `rust/crates/tx/src/hosted_control.rs:18`
- `rust/crates/tx/src/hosted_control.rs:21`
- `rust/crates/tx/src/hosted_control.rs:198`
- `rust/crates/tx/src/hosted_control.rs:200`
- `rust/crates/tx/src/hosted_control.rs:612`
- `rust/crates/tx/src/hosted_control.rs:738`
- `rust/crates/tx/src/hosted_control.rs:887`
- `rust/crates/tx/src/hosted_control.rs:931`
- `rust/crates/tx/src/hosted_control.rs:941`
- `rust/crates/tx/src/hosted_control.rs:1024`
- `rust/crates/tx/src/hosted_control.rs:1052`
- `rust/crates/tx/src/hosted_control.rs:1141`
- `rust/crates/tx/src/hosted_control.rs:1238`
- `rust/crates/tx/src/hosted_control.rs:1275`
- `rust/crates/tx/src/hosted_control.rs:1321`
- `rust/crates/tx/src/hosted_control.rs:1378`
- `rust/crates/tx/src/hosted_control.rs:1468`
- `rust/crates/tx/src/hosted_control.rs:1510`
- `rust/crates/tx/src/hosted_control.rs:1651`
- `rust/crates/tx/src/hosted_control.rs:1655`
- `rust/crates/tx/src/hosted_control.rs:1775`
- `rust/crates/tx/src/hosted_control/world_readmission.rs:7`
- `rust/crates/tx/src/hosted_control/world_readmission.rs:72`
- `rust/crates/tx/src/replica.rs:371`
- `rust/crates/tx/src/resolver.rs:4000`
- `rust/crates/tx/src/resolver.rs:4053`
- `rust/crates/tx/src/resolver.rs:4065`
- `rust/crates/tx/src/resolver.rs:4102`
- `rust/crates/tx/src/resolver/constraint_receipt.rs:7`
- `rust/crates/tx/src/resolver/constraint_receipt.rs:10`
- `rust/crates/tx/src/resolver/constraint_receipt.rs:11`
- `rust/crates/tx/src/resolver/hosted_control.rs:103`
- `rust/crates/tx/src/resolver/hosted_control.rs:253`
- `rust/crates/tx/src/resolver/owned_capacity.rs:405`
- `rust/crates/tx/src/resolver/owned_capacity.rs:482`
- `rust/crates/tx/src/resolver/replay_worlds.rs:446`
- `rust/crates/tx/src/resolver/replay_worlds.rs:461`
- `rust/crates/tx/src/resolver/replay_worlds.rs:589`
- `rust/crates/tx/src/resolver/schedule_order.rs:11`
- `rust/crates/tx/src/source.rs:2504`
- `rust/crates/tx/src/source.rs:2521`
- `rust/crates/types/src/id.rs:214`
- `verification/multi/held-worlds-order.md:61`
- `verification/owned-landing-wire.md:6`
