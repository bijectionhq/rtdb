import Lake
open Lake DSL

package «rtdb-installed-erasure»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "2fba3b1391cf571d9779254a006f9ec013c7e00a" / "verification/lean"

lean_lib ErasureCommit where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "112-erasure-rotation"

@[default_target]
lean_lib InstalledErasureCommit
