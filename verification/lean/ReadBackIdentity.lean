import Std

namespace RTDB.ReadBackIdentity

abbrev Frame := List UInt8

structure Pending where
  sequence : Nat
  staged : Nat
  frame : Frame

structure Delivery where
  sequence : Option Nat
  resolved : Bool
  frame : Frame

def confirm [DecidableEq Digest] (hash : Frame → Digest) (pending : Pending)
    (delivery : Delivery) : Bool :=
  decide (delivery.sequence = some pending.sequence ∧ delivery.resolved = true ∧
    hash delivery.frame = hash pending.frame)

def publish [DecidableEq Digest] (hash : Frame → Digest) (pending : Pending)
    (delivery : Delivery) (head : Option Nat) : Option Nat :=
  if confirm hash pending delivery = true ∧ head = some pending.staged then
    some pending.staged
  else none

theorem missing_sequence_refuses [DecidableEq Digest] (hash : Frame → Digest) (pending : Pending)
    (delivery : Delivery) (missing : delivery.sequence = none) :
    confirm hash pending delivery = false := by
  simp [confirm, missing]

theorem control_at_pending_sequence_refuses [DecidableEq Digest] (hash : Frame → Digest)
    (pending : Pending) (delivery : Delivery) (control : delivery.resolved = false) :
    confirm hash pending delivery = false := by
  simp [confirm, control]

theorem confirmation_proves_equal_frames_given_no_collision [DecidableEq Digest]
    (hash : Frame → Digest) (pending : Pending) (delivery : Delivery)
    (confirmed : confirm hash pending delivery = true)
    (noCollision : hash delivery.frame = hash pending.frame → delivery.frame = pending.frame) :
    delivery.sequence = some pending.sequence ∧ delivery.frame = pending.frame := by
  simp only [confirm, decide_eq_true_eq] at confirmed
  constructor
  · grind
  · apply noCollision
    grind

theorem publication_uses_the_confirmed_queue_head [DecidableEq Digest] (hash : Frame → Digest)
    (pending : Pending) (delivery : Delivery) (head : Option Nat) (published : Nat)
    (success : publish hash pending delivery head = some published) :
    confirm hash pending delivery = true ∧ head = some pending.staged ∧ published = pending.staged := by
  simp only [publish] at success
  split at success
  next checked => grind
  next => cases success

theorem matching_append_and_head_can_publish [DecidableEq Digest] (hash : Frame → Digest)
    (pending : Pending) :
    publish hash pending ⟨some pending.sequence, true, pending.frame⟩ (some pending.staged) =
      some pending.staged := by
  simp [publish, confirm]

#print axioms missing_sequence_refuses
#print axioms control_at_pending_sequence_refuses
#print axioms confirmation_proves_equal_frames_given_no_collision
#print axioms publication_uses_the_confirmed_queue_head
#print axioms matching_append_and_head_can_publish

end RTDB.ReadBackIdentity
