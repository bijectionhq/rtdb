import Lake
open Lake DSL

package «rtdb-multi-recovery»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "0bc92900832554ec83787ec00e4843773734f4a5" / "verification/lean"

lean_lib OrderedModels where
  srcDir := "lean"
  roots := #[`OrderedCapacity, `OrderedJournal, `OrderedQuorum]

@[default_target]
lean_lib RecoveryComposition where
  srcDir := "compose"
  roots := #[`RecoveryComposition]
