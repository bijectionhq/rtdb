# Structural redesign: designs and handoff notes

The structural redesign separates transaction identity, serving authority, residence holds, key custody and the SQL scoring contract.
The first review established R1–R9.
The second review revises the custody and erasure contracts and adds the placed serving and search work.

## Status

| Increment | Note | State on main |
| --- | --- | --- |
| R1 System time bounded by the source log | — | Landed `d85286ba2` |
| R2 Engine-owned retry receipts | — | Landed `85a868c07` (receipts are bitemporal rows in `xt.idempotency_receipts`; R8 removes them physically) |
| R3 Epoch-qualified physical names | — | Landed `ae7b66e10` (naming epoch only; an input digest proved unnecessary) |
| R4 Residence ledger, holds and collection floor | [R4-residence-ledger.md](R4-residence-ledger.md) | Multi and native local storage enforce a catalog admission floor and retry retirement from installed receipts. Remote classic storage, renewable holds and timer removal remain. |
| R5 Recovery roots; collectable L0 | [R5-recovery-roots.md](R5-recovery-roots.md) | Multi commits its replay inventory and suffix together before trim and trie-file retirement, and archive copies protect their journal range through provider drain. Offline native repair rebuilds after source checkpoint and L0 loss, and a retained local export can protect its successor metadata and offloaded replay payloads across restarts. Online reset and automatic L0 collection still need held known-good cuts, and terminal pack coverage and provider qualification remain open. |
| R6 Sealing with destroyable data keys | [R6-sealing.md](R6-sealing.md) | Multi seals under a per-deployment key; classic unsealed; no per-unit keys |
| R7 History collection and privacy erasure | [R7-eraser.md](R7-eraser.md) | Contract and bounded custody model; runtime pending |
| R8 Retention of receipts and transaction metadata | [R8-metadata-retention.md](R8-metadata-retention.md) | Ordered expiry and receipt compaction land. New checkpoints omit covered receipt tries. Opted-in hosted directory deployments retire exact loose receipt L0 versions after successor publication and trim. Closure over remaining copies stays open. |
| R9 Capabilities and composed model | [R9-capabilities-and-composition.md](R9-capabilities-and-composition.md) | Replica admission and lease scopes are implemented. Live cell recovery and trim traces are checked. The remaining authority transitions need production capture. |

Dependency order: R4 and R6 can proceed in parallel; R5 needs R4; R7 needs R5 and R6; R8 needs R7; R9 grows with each.

## Contract choices

- A client system time may not lead the source log's append time.
- A basis is a logical coordinate; an admitted handle is never resumed, and clients re-admit at the same basis.
- A view admitted before an erasure keeps its answers until the erasure deadline (at most 120 s after the erase commits), then fails with `rtdb/erased-under-view`; SNAP-1 and DUR-1 are amended accordingly.
- Privacy erasure reaches forks sharing the custody domain.
  Independent exports and archives use independent keys and obligations.
- Retry identities are scoped to (database, principal, key) with a body digest; transaction metadata is retained only for the receipt window.
- Physical erasure (OPT-2) is in scope with a declared bound; non-interference (OPT-3) only for rows and search scores of fresh views.
- XTDB wire, configuration and storage compatibility is not a constraint; hard cutovers are allowed.

## Coordinates (shared by every note)

- **History** uses transaction system time, bounded by the log clock. No physical decision reads it.
- **Reclamation** uses a monotone catalog generation recorded by the residence ledger.
- **Liveness of holders** uses node-local monotonic durations only, to expire holds that stop renewing.
- **Receipt expiry** uses the recorded log time.
- **Erasure deadlines** use the custody grant clock and start at commit.

## Second review

| Change | Target | State |
| --- | --- | --- |
| Placed attempt receipts, fresh reads and loglet fences | [Authority boundaries](authority-boundaries.md) | Opt-in subscriber reads use durable loglet indexes. Placed submissions issue scoped receipts and redeem them against installed outcomes. Frozen readers confirm receipts against their admitted checkpoint. Retiring lease-based self-fencing remains open. |
| CDC recoverability and upstream acknowledgement | [Authority boundaries](authority-boundaries.md#recovery-roots-and-upstream-acknowledgement) | Native PostgreSQL can confirm early from retained, synced local journals and checkpoint storage. Hosted early confirmation needs provider recovery authority. |
| Argument-defined search ranking | [Full-text search](../../dev/doc/fts.allium) | Implemented in native SQL and the rank accelerator |
| Custody-scoped sealing, enrollment and erasure | [R6](R6-sealing.md) · [R7](R7-eraser.md) | Bounded custody protocol checked in TLA+; provider and runtime changes pending |

## Working rules for whoever picks one up

- Read `verification/PROGRAMME.md` (ground rules, toolchain, runner) and `AGENTS.md` first.
- Before starting, read the last day of `git log origin/main` for the crates the note names. Peer sessions land work on R4 and R5 hourly; extend their mechanisms instead of building a parallel one.
- Rebuild after every rebase before pushing; peers occasionally land commits that do not compile.
- Each increment lands with its tests, its spec changes (`dev/doc/*.allium`, `allium/*.allium`), a ledger entry in `verification/results/2026-11-21.adoc`, and a status update in `dev/doc/structural-redesign.adoc` and the table above.
- Every claim needs a passing baseline and a mutation that fails at the declared law or invariant.
