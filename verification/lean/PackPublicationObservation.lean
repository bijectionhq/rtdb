import Std

namespace RTDB.PackPublicationObservation

inductive Phase (Version : Type) where
  | head (head : Version)
  | trailer (head trailer : Version)
  | index (head trailer index : Version)
  | returned (head trailer index member : Version)

structure Read (Version : Type) where
  current : Version
  phase : Phase Version

inductive Step {Version : Type} : Read Version → Read Version → Prop where
  | provider (phase) (old new : Version) : Step ⟨old, phase⟩ ⟨new, phase⟩
  | trailer (current head : Version) (trailerMatches : head = current) :
      Step ⟨current, .head head⟩ ⟨current, .trailer head current⟩
  | index (current head trailer : Version) (indexMatches : head = current) :
      Step ⟨current, .trailer head trailer⟩ ⟨current, .index head trailer current⟩
  | member (current head trailer index : Version) (memberMatches : index = current) :
      Step ⟨current, .index head trailer index⟩ ⟨current, .returned head trailer index current⟩

inductive Schedule {Version : Type} : Read Version → Read Version → Prop where
  | same (read) : Schedule read read
  | next {start middle finish} : Schedule start middle → Step middle finish → Schedule start finish

def coherent {Version : Type} : Phase Version → Prop
  | .head _ => True
  | .trailer head trailer => head = trailer
  | .index head trailer index => head = trailer ∧ head = index
  | .returned head trailer index member => head = trailer ∧ head = index ∧ index = member

theorem each_step_preserves_publication {Version : Type} {before after : Read Version}
    (step : Step before after) (valid : coherent before.phase) : coherent after.phase := by
  cases step with
  | provider => exact valid
  | trailer _ _ matched => exact matched
  | index _ _ _ matched => exact ⟨valid, matched⟩
  | member _ _ _ _ matched => exact ⟨valid.1, valid.2, matched⟩

theorem all_finite_schedules_preserve_publication {Version : Type} {start finish : Read Version}
    (schedule : Schedule start finish) (valid : coherent start.phase) : coherent finish.phase := by
  induction schedule with
  | same => exact valid
  | next _ step preserved => exact each_step_preserves_publication step preserved

#print axioms each_step_preserves_publication
#print axioms all_finite_schedules_preserve_publication

end RTDB.PackPublicationObservation
