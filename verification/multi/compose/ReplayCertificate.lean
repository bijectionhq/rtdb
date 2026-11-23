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

#print axioms every_base_read_has_exact_held_coverage
#print axioms every_reachable_replay_demand_has_exact_held_coverage
#print axioms one_az_placement_covers_every_reachable_replay_demand

end RTDB.Recovery.Certificate
