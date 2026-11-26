import Lake
open Lake DSL

package «rtdb-installed-erasure»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "a2059e33063869675c726d3a3856cc362c684e74" / "verification/lean"

lean_lib ErasureCommit where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "112-erasure-rotation"

@[default_target]
lean_lib InstalledErasureCommit
