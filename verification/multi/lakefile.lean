import Lake
open Lake DSL

package «rtdb-multi-recovery»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "749260efc462527b31609f6a3d22ae240a9e8b04" / "verification/lean"

lean_lib OrderedModels where
  srcDir := "lean"
  roots := #[`CheckpointReplay, `OrderedCapacity, `OrderedJournal, `OrderedQuorum, `ReplayClosure, `HeldWorldsOrder, `FencedTermAdmission, `MaintenanceClassification, `InstalledJournalGuards, `WorkerRoster]

lean_lib MaintenanceBound where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "111-maintenance-liveness"

lean_lib DisposalLease where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "113-worker-disposal"

lean_lib ReaderRetention where
  srcDir := "../lean"

@[default_target]
lean_lib RecoveryComposition where
  srcDir := "compose"
  roots := #[`InstalledRead, `NativeAppend, `RecoveryComposition, `ReplayCertificate, `AcknowledgedRecovery, `AcknowledgedRecoveryControls,
    `InstalledJournalProgress, `NativeAcknowledgement, `ExactReclamation, `SurvivorCheckpoint, `ForkCustody,
    `ReplicationDriver, `ReplicationDriverTraces]
