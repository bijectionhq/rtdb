import Lake
open Lake DSL

package «rtdb-installed-erasure»

require logkeeper from git
  "https://github.com/bijectionhq/logkeeper.git" @ "354795cc467138b07bbeebed95af526b1ab21798" / "verification/lean"

lean_lib ErasureCommit where
  srcDir := defaultPackagesDir / "logkeeper" / "verification" / "112-erasure-rotation"

@[default_target]
lean_lib InstalledErasureCommit
