# Structural redesign: designs and handoff notes

Six guarantees could not be closed in the inherited architecture (see `dev/doc/structural-redesign.adoc`).
Three independent designers converged on one redesign; the owner settled the open choices on 2026-11-21.
This directory holds one design-and-handoff note per remaining increment, written so an agent can pick one up cold.

## Status

| Increment | Note | State on main |
| --- | --- | --- |
| R1 System time bounded by the source log | — | Landed `d85286ba2` |
| R2 Engine-owned retry receipts | — | Landed `85a868c07` (receipts are bitemporal rows in `xt.idempotency_receipts`; R8 removes them physically) |
| R3 Epoch-qualified physical names | — | Landed `ae7b66e10` (naming epoch only; an input digest proved unnecessary) |
| R4 Residence ledger, holds and collection floor | [R4-residence-ledger.md](R4-residence-ledger.md) | Multi reader registrations landed by a peer (`dd5a0b0fa`); classic not started |
| R5 Recovery roots; collectable L0 | [R5-recovery-roots.md](R5-recovery-roots.md) | Multi recovery and checkpoint-metadata holds exist; roots not yet the recovery contract |
| R6 Sealing with destroyable data keys | [R6-sealing.md](R6-sealing.md) | Multi seals under a per-deployment key; classic unsealed; no per-unit keys |
| R7 Eraser | [R7-eraser.md](R7-eraser.md) | Not started |
| R8 Retention of receipts and transaction metadata | [R8-metadata-retention.md](R8-metadata-retention.md) | Not started |
| R9 Capabilities and composed model | [R9-capabilities-and-composition.md](R9-capabilities-and-composition.md) | Pattern exists (replica admission witness, `IoRegion` lease scope) |

Dependency order: R4 and R6 can proceed in parallel; R5 needs R4; R7 needs R5 and R6; R8 needs R7; R9 grows with each.

## Owner decisions every note assumes

- A client system time may not lead the source log's append time.
- A basis is a logical coordinate; an admitted handle is never resumed, and clients re-admit at the same basis.
- A view admitted before an erasure keeps its answers until the erasure deadline (at most 120 s after the erase commits), then fails with `rtdb/erased-under-view`; SNAP-1 and DUR-1 are amended accordingly.
- An erasure also erases the material in forks whose history includes it.
- Retry identities are scoped to (database, principal, key) with a body digest; transaction metadata is retained only for the receipt window.
- Physical erasure (OPT-2) is in scope with a declared bound; non-interference (OPT-3) only for rows and search scores of fresh views.
- XTDB wire, configuration and storage compatibility is not a constraint; hard cutovers are allowed.

## Coordinates (shared by every note)

- **History** uses transaction system time, bounded by the log clock. No physical decision reads it.
- **Reclamation** uses a monotone catalog generation recorded by the residence ledger.
- **Liveness of holders** uses node-local monotonic durations only, to expire holds that stop renewing.
- **Receipt expiry and erasure deadlines** use the log time recorded with each record.

## Working rules for whoever picks one up

- Read `verification/PROGRAMME.md` (ground rules, toolchain, runner) and `AGENTS.md` first.
- Before starting, read the last day of `git log origin/main` for the crates the note names. Peer sessions land work on R4 and R5 hourly; extend their mechanisms instead of building a parallel one.
- Rebuild after every rebase before pushing; peers occasionally land commits that do not compile.
- Each increment lands with its tests, its spec changes (`dev/doc/*.allium`, `allium/*.allium`), a ledger entry in `verification/results/2026-11-21.adoc`, and a status update in `dev/doc/structural-redesign.adoc` and the table above.
- Every claim needs a passing baseline and a mutation that fails at the declared law or invariant.
