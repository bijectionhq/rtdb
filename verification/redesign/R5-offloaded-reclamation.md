# Live reclamation of offloaded replica payloads

## Counterexample

A recovery-tail hold reserved the suffix through the tip sampled at opening.
An ordinary follower then received an oversized reference after that tip and paused before loading the object.
The holder installed a checkpoint and trimmed the pointer without seeing an active hold.
Age-based deletion could strand the follower even when it named the exact object version.
A second schedule loses an append reply after commit and therefore needs to discover its publication from the certified history.
A third schedule restarts after physical trim or provider deletion but before receiving the reply.

## Approved production boundary

1. Bind every detached payload to the authenticated frame, original history, installation incarnation, stream, committed position and immutable provider identity.
   Persist that binding and its candidate evidence before retiring any record needed to reconstruct it.
   An ambiguous append remains in staging custody until commitment is certified or the writer is fenced against later acceptance.
2. Verify a durable successor checkpoint and full recovery closure through the acknowledged prefix, with sufficient redundant custody and the authentic resume token.
   Select the covering route and install the recovery dependency fence in the same non-rollback authority.
   The fence binds the original stream and interval across physical journals, including a journal discovered or restored later.
3. Require a brokered, position-scoped capability for delivery, replay, repair, archive, fork and transfer before dereferencing a frame.
   The broker mediates the complete data and key path.
   Retain admitted work through drain and refuse new access through excluded pointers.
   A returned journal rejoins under current authority with verified coverage.
4. Issue one deletion grant per unreachable immutable publication once all actual dependencies drain and future use is excluded.
   Provider settlement consumes the same grant after interruption or ambiguous replies.
   Physical journal trim and media destruction have their own authorities and settlement evidence.

The directory-managed versioned S3 broker records its deletion grant against the installed checkpoint and certified recovery fence.
It waits for matching retained replay ranges and exact upload attempts to drain, withdraws the body key, then confirms absence of the granted provider version.
Active-quorum trimming and journal-frame key withdrawal are handled separately.
The production fence still selects a contiguous prefix and can wait on an earlier unrelated hold.
The bounded model still represents physical trim in the original settlement sequence.

## Bounded check

[OffloadedReplicaReclamation](../tla/OffloadedReplicaReclamation.tla) models one ambiguous append, one prior-version reader, one ordinary live reader, and two versions at a provider key.
It treats the certified committed binding, exclusion of new legacy admissions, and exact provider effects as transitions.
TLC exhausts 80 distinct states.
The eight controls reject an unbound append, missing reader tracking and compatibility fences, deletion on trim authorization, lost retirement intent, and deletion by key.
The reachability cases settle after a lost append acknowledgement and restart between the physical trim and deletion, and after restart with the delete already applied.
