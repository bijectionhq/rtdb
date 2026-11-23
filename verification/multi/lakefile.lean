import Lake
open Lake DSL

package «rtdb-multi-recovery»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "006c7da7ce815d6149beb8dfbf4ff6d0780bc81e" / "verification/lean"

lean_lib OrderedModels where
  srcDir := "lean"
  roots := #[`OrderedCapacity, `OrderedJournal, `OrderedQuorum]

@[default_target]
lean_lib RecoveryComposition where
  srcDir := "compose"
  roots := #[`RecoveryComposition]
