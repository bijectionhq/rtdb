import Lake
open Lake DSL

package «rtdb-multi-recovery»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "895bc30891e954c585c9be5ee135c6b4b5b660a1" / "verification/lean"

lean_lib OrderedModels where
  srcDir := "lean"
  roots := #[`OrderedCapacity, `OrderedJournal, `OrderedQuorum, `ReplayClosure]

@[default_target]
lean_lib RecoveryComposition where
  srcDir := "compose"
  roots := #[`RecoveryComposition, `ReplayCertificate]
