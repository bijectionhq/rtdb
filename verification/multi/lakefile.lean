import Lake
open Lake DSL

package «rtdb-multi-recovery»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "11caa64b52d77ab741553aa0e0599f85e68961d0" / "verification/lean"

lean_lib OrderedModels where
  srcDir := "lean"
  roots := #[`OrderedCapacity, `OrderedJournal, `OrderedQuorum, `ReplayClosure]

@[default_target]
lean_lib RecoveryComposition where
  srcDir := "compose"
  roots := #[`RecoveryComposition, `ReplayCertificate]
