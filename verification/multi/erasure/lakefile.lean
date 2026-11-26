import Lake
open Lake DSL

package «rtdb-installed-erasure»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "1e8a0df7d0b88bcb9946328592b7084d26b5ec64" / "verification/lean"

lean_lib ErasureCommit where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "112-erasure-rotation"

@[default_target]
lean_lib InstalledErasureCommit
