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

[OffloadedReplicaReclamation](../tla/OffloadedReplicaReclamation.tla) models an ambiguous append, a live draining reader, an unrelated held position, an absent historical journal and two physical versions at a provider key.
The model requires a complete successor before closing replay admission. It orders the exact grant, key withdrawal, physical removal and settlement independently from active journal trim.
The controls cover missing successor members, bypassed drain, stale admission, deletion before withdrawal, grant loss and deletion by key.
The progress controls use an unrelated hold and a permanently absent journal. The crash witnesses cover both a lost append acknowledgement and an applied deletion whose reply was lost.
The revised model and controls have not been run.

The directory keeps the certified detached dependency boundary separately from the physical journal trim anchor.
A successor checkpoint permits the census to advance past a finite replay ticket while the broker keeps the positions covered by that ticket.
The broker can redeem an already admitted dependency using the physical journal anchor, the original ticket and the exact frame proof.
A new replay cannot open below the detached boundary. A copy from a registered suffix pin inherits its original authenticated interval.
The pending census is bounded. Open-ended recovery scopes still protect their ranges through drain.

Ordinary hosted writer uploads enter the directory before provider effects.
Enqueue first orders the exact pointer digest at the broker.
A certified positive receipt transfers custody to the committed journal. After response loss the broker retains and scans the uninspected interval.
A newer confirmed or sealed journal epoch fences the original writer. Once the broker proves the whole retained interval lacks the pointer and joins provider drain, it records a grant for the exact staged version and settles it.
Transfer stages enter a separate receipt protocol and still need terminal settlement for bodies left before seeding.


### Detached transfer staging

A checkpoint transfer binds the destination staging operation, certified bootstrap and replica position before the provider upload.
A cell move binds the destination generation and position.
The broker keeps the provider attempt until a certified destination frame adopts that exact version or the directory closes the destination authority and outstanding provider work drains.
The producer confirms bounded pages before submitting more drafts.
A lost upload or journal reply can recover the recorded attempt. A retry after adoption must prove that the pre-existing version is already named by the designated journal position.
The directory cannot activate a closed generation, and an excluded journal pointer no longer opens a body through the broker.
