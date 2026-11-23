import Lake
open Lake DSL

package «rtdb-multi-recovery»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "818f38415f65a704949cf20240e2f2e2512cb89b" / "verification/lean"

lean_lib OrderedModels where
  srcDir := "lean"
  roots := #[`CheckpointReplay, `OrderedCapacity, `OrderedJournal, `OrderedQuorum, `ReplayClosure, `HeldWorldsOrder, `FencedTermAdmission, `MaintenanceBound]

lean_lib MaintenanceBound where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "111-maintenance-liveness"

lean_lib DisposalLease where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "113-worker-disposal"

lean_lib ReaderRetention where
  srcDir := "../lean"

@[default_target]
lean_lib RecoveryComposition where
  srcDir := "compose"
  roots := #[`RecoveryComposition, `ReplayCertificate, `AcknowledgedRecovery, `AcknowledgedRecoveryControls,
    `InstalledJournalProgress]
