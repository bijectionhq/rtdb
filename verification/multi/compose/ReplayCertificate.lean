import Logkeeper.RecoveryCertificate
import ReplayClosure

set_option warningAsError true

/-
RTDB instantiates Logkeeper's opaque recovery certificate with its typed
replay closure. A certificate that is ready for the enumerated keys covers
every read and reference made by any reachable replay of the chosen suffix,
under every credible fault.
-/

namespace RTDB.Recovery.Certificate

open Logkeeper.RecoveryCertificate
open RTDB.Recovery.ReplayClosure

def dependencies : Dependencies Selection Key :=
  ⟨recoveryKeys, ReplayRead, enumeration_covers_every_base_and_suffix_read⟩

theorem every_base_read_has_exact_held_coverage {Failure : Type}
    (survives : PhysicalVersion → Failure → Prop) (inventory : Key → PhysicalVersion → Prop)
    (grants : Grant Key → Prop) (scope : Scope) (selected : Selection) (credible : Failure → Prop)
    (ready : Ready dependencies survives inventory grants scope selected credible)
    (base : Checkpoint) (installed : selected.checkpoint = some base)
    (key : Key) (read : BaseRead base key) (fault : Failure) (allowed : credible fault) :
    ∃ physical, inventory key physical ∧ survives physical fault ∧
      ∃ grant, grants grant ∧ Binds scope key physical grant :=
  every_application_read_has_exact_held_coverage dependencies survives inventory grants scope selected
    credible ready key (.inl ⟨base, installed, read⟩) fault allowed

theorem every_reachable_replay_demand_has_exact_held_coverage {Failure : Type}
    (survives : PhysicalVersion → Failure → Prop) (inventory : Key → PhysicalVersion → Prop)
    (grants : Grant Key → Prop) (scope : Scope) (selected : Selection) (credible : Failure → Prop)
    (ready : Ready dependencies survives inventory grants scope selected credible)
    {state : Replay} (reached : Reaches selected state)
    (key : Key) (demanded : key ∈ state.demands) (fault : Failure) (allowed : credible fault) :
    ∃ physical, inventory key physical ∧ survives physical fault ∧
      ∃ grant, grants grant ∧ Binds scope key physical grant :=
  ready key (List.mem_append_right _
    (every_replay_read_is_in_the_typed_tail_enumeration selected reached key demanded)) fault allowed

theorem one_az_placement_covers_every_reachable_replay_demand {Zone : Type}
    (zoneOf : PhysicalVersion → Zone) (inventory : Key → PhysicalVersion → Prop)
    (grants : Grant Key → Prop) (scope : Scope) (selected : Selection) (credible : Zone → Prop)
    (ready : ZoneReady dependencies zoneOf inventory grants scope selected credible)
    {state : Replay} (reached : Reaches selected state)
    (key : Key) (demanded : key ∈ state.demands) (lost : Zone) (allowed : credible lost) :
    ∃ physical, inventory key physical ∧ zoneOf physical ≠ lost ∧
      ∃ grant, grants grant ∧ Binds scope key physical grant :=
  every_reachable_replay_demand_has_exact_held_coverage _ inventory grants scope selected credible
    (zone_placement_refines_the_provider_contract dependencies zoneOf inventory grants scope selected
      credible ready) reached key demanded lost allowed

/-
An RTDB Multi journal carries its replay base separately from its floor. The
floor is the first retained LSN. The base is the LSN of the record that
announced the installed checkpoint: the floor itself for a classic base, or a
later retained record for a deferred base (`ReplayBase` in
`rust/crates/db/src/replay.rs`; `ReplayClosure.DeferredBase` proves the
replay equal to replay from LSN 1). Coverage is a property of the selected
checkpoint and suffix, so a deferred base needs exactly the coverage of a
classic one.
-/
structure JournalSelection where
  selection : Selection
  floor : Nat
  base : Option Nat
  retained : ∀ lsn, base = some lsn → floor ≤ lsn
  installed : base.isSome = selection.checkpoint.isSome

def JournalSelection.deferred (journal : JournalSelection) : Prop :=
  ∃ lsn, journal.base = some lsn ∧ journal.floor < lsn

theorem a_deferred_base_is_retained_after_its_floor (journal : JournalSelection)
    (deferred : journal.deferred) : ∃ lsn, journal.base = some lsn ∧ journal.floor ≤ lsn :=
  let ⟨lsn, based, later⟩ := deferred
  ⟨lsn, based, Nat.le_of_lt later⟩

theorem every_read_of_a_deferred_base_has_exact_held_coverage {Failure : Type}
    (survives : PhysicalVersion → Failure → Prop) (inventory : Key → PhysicalVersion → Prop)
    (grants : Grant Key → Prop) (scope : Scope) (journal : JournalSelection) (credible : Failure → Prop)
    (ready : Ready dependencies survives inventory grants scope journal.selection credible)
    (lsn : Nat) (_announced : journal.base = some lsn)
    (base : Checkpoint) (installed : journal.selection.checkpoint = some base)
    (key : Key) (read : BaseRead base key) (fault : Failure) (allowed : credible fault) :
    ∃ physical, inventory key physical ∧ survives physical fault ∧
      ∃ grant, grants grant ∧ Binds scope key physical grant :=
  every_base_read_has_exact_held_coverage survives inventory grants scope journal.selection credible
    ready base installed key read fault allowed

theorem moving_the_announcement_keeps_the_certificate {Failure : Type}
    (survives : PhysicalVersion → Failure → Prop) (inventory : Key → PhysicalVersion → Prop)
    (grants : Grant Key → Prop) (scope : Scope) (classic deferred : JournalSelection)
    (credible : Failure → Prop) (same : classic.selection = deferred.selection)
    (ready : Ready dependencies survives inventory grants scope classic.selection credible) :
    Ready dependencies survives inventory grants scope deferred.selection credible :=
  same ▸ ready

#print axioms every_base_read_has_exact_held_coverage
#print axioms every_reachable_replay_demand_has_exact_held_coverage
#print axioms one_az_placement_covers_every_reachable_replay_demand
#print axioms a_deferred_base_is_retained_after_its_floor
#print axioms every_read_of_a_deferred_base_has_exact_held_coverage
#print axioms moving_the_announcement_keeps_the_certificate

end RTDB.Recovery.Certificate
