import Std

namespace RTDB.Recovery.GatewayReplay

inductive MetadataKey where
  | absent
  | malformed
  | application (key : String)

structure Backend where
  commandKey : String
  ordinal : Nat
  sqlSettings : List (String × String)

def resetSettings (backend : Backend) : Backend :=
  { backend with sqlSettings := [] }

def effectiveKey (backend : Backend) (metadata : MetadataKey) : String :=
  match metadata with
  | .application key => key
  | _ => backend.commandKey ++ "/" ++ toString backend.ordinal

theorem reset_preserves_effective_key (backend : Backend) (metadata : MetadataKey) :
    effectiveKey (resetSettings backend) metadata = effectiveKey backend metadata := by
  cases metadata <;> rfl

theorem malformed_metadata_uses_command_key (backend : Backend) :
    effectiveKey backend .malformed = effectiveKey backend .absent := rfl

theorem reinstall_restores_transaction_key
    (first replacement : Backend)
    (sameKey : first.commandKey = replacement.commandKey)
    (sameOrdinal : first.ordinal = replacement.ordinal)
    (metadata : MetadataKey) :
    effectiveKey first metadata = effectiveKey replacement metadata := by
  cases metadata <;> simp [effectiveKey, sameKey, sameOrdinal]

def cutoff (window : Nat) (firstFailure : Nat) : Nat :=
  firstFailure + window

def startRecovery (deadline : Option Nat) (now window : Nat) : Nat :=
  deadline.getD (cutoff window now)

def replaceWriters (deadline : Nat) (window : Nat) : List Nat → Nat
  | [] => deadline
  | now :: rest => replaceWriters (startRecovery (some deadline) now window) window rest

theorem replacements_preserve_cutoff (deadline window : Nat) (times : List Nat) :
    replaceWriters deadline window times = deadline := by
  induction times with
  | nil => rfl
  | cons now rest ih => simpa [replaceWriters, startRecovery] using ih

theorem recovery_window_is_measured_from_first_failure
    (firstFailure window now : Nat) (replacements : List Nat) :
    now < replaceWriters (startRecovery none firstFailure window) window replacements
      ↔ now < firstFailure + window := by
  rw [replacements_preserve_cutoff]
  rfl

#print axioms reset_preserves_effective_key
#print axioms malformed_metadata_uses_command_key
#print axioms reinstall_restores_transaction_key
#print axioms recovery_window_is_measured_from_first_failure

end RTDB.Recovery.GatewayReplay
