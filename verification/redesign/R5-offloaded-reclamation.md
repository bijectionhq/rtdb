# Live reclamation of offloaded replica payloads

## Counterexample

A recovery-tail hold reserved the suffix through the tip sampled at opening.
An ordinary follower then received an oversized reference after that tip and paused before loading the object.
The holder installed a checkpoint and trimmed the pointer without seeing an active hold.
Age-based deletion could strand the follower even when it named the exact object version.
A second schedule loses an append reply after commit and therefore needs to discover its publication from the certified history.
A third schedule restarts after physical trim or provider deletion but before receiving the reply.

## Required production boundary

1. Bind a distinct allocation's immutable provider version to the authenticated oversized frame and its committed journal position.
   Keep the provider completion, frame digest, incarnation, journal and storage scope together.
   Persist the candidate(s) with the certified prefix before any record needed to reconstruct them can be trimmed.
   A committed frame found during certification qualifies even if append acknowledgement was lost.
   An absent pending append remains outside this collector until a separate durable fence establishes that it cannot commit later.
2. Give ordinary tails and finite replay or copy readers a position-scoped hold before delivery.
   The replay authority rejects a new hold below the authorized frontier.
   Decoding, payload fetch and cancellation retain the same hold until provider drain.
   A compatibility cut must also close new admissions for legacy readers and drain all existing legacy scopes that could still use the object.
   Every participating maintenance binary must exclude offloaded payloads from age-based collection.
3. Persist retirement intent and the exact covered bound before calling logkeeper trim.
   Match physical trim evidence to the history and journal identities of the authorization, including any old journal that could still supply permitted reads.
   On restart, re-observe or idempotently settle the same physical trim before publishing a deletion grant.
4. Retire only the provider version named by that grant.
   Require the same holds to protect inherited or transferred versions until independent custody has been verified.
   Keep the intent through provider confirmation of that version's absence.
   The qualified collector leaves legacy payloads without a publication witness retained.

The directory owns the paired retirement intent and admission frontier.
A replay hold and a deletion grant are immutable values owned by the reader region or settlement operation that consumes them.
The logkeeper and versioned object provider execute their respective effects.
The reaper schedules retries while the durable intent remains present.

The serving path now retains each offloaded tail or range delivery through its provider operation.
The certified versioned-S3 path installs exact candidates in the directory compare-and-swap that advances its admission frontier.
The placement owner persists completion after every known physical journal returns a synchronous trim acknowledgement.
Legacy and non-fsync journal replies cannot satisfy this step.
The lineage reaper matches each release against that durable candidate and retries settlement against the same provider version.
A bounded outstanding inventory stops trim before it loses unretired candidates.
The opt-in rollout requires every serving and recovery reader to support the new envelope.
Local objects, unversioned buckets and references without an exact publication witness stay outside this collection path.

## Bounded check

[OffloadedReplicaReclamation](../tla/OffloadedReplicaReclamation.tla) models one ambiguous append, one prior-version reader, one ordinary live reader, and two versions at a provider key.
It treats the certified committed binding, exclusion of new legacy admissions, and exact provider effects as transitions.
TLC exhausts 80 distinct states.
The eight controls reject an unbound append, missing reader tracking and compatibility fences, deletion on trim authorization, lost retirement intent, and deletion by key.
The reachability cases settle after a lost append acknowledgement and restart between the physical trim and deletion, and after restart with the delete already applied.
