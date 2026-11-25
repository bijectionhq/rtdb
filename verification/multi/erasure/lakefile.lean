import Lake
open Lake DSL

package «rtdb-installed-erasure»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "b12e0e5fef040d5079723bc6b4d73bb49d81ab26" / "verification/lean"

lean_lib ErasureCommit where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "112-erasure-rotation"

@[default_target]
lean_lib InstalledErasureCommit
