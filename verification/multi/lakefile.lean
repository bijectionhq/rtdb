import Lake
open Lake DSL

package «rtdb-multi-recovery»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "2af6d2937c36755ae3efd23fae7fdb62d13cfa27" / "verification/lean"

lean_lib OrderedModels where
  srcDir := "lean"
  roots := #[`OrderedCapacity, `OrderedJournal, `OrderedQuorum, `ReplayClosure]

@[default_target]
lean_lib RecoveryComposition where
  srcDir := "compose"
  roots := #[`RecoveryComposition, `ReplayCertificate]
