# Installed replay cursor verification

The checked source is `e31980f2a969f287b848317f80e37c96643e45eb`.
The proof inputs were captured from that source and checked under owner-authorized local execution.

Verus 0.2026.11.05.31e44e6 with Rust 1.99.1 and Z3 4.16.0 checked the three areas through `verification/run.py::verify_verus`.

| Area | Baseline verified items | Controls rejected at their declared diagnostics |
| --- | ---: | ---: |
| `ordered_recovery_cursor`, including `laws::exact_delivery_retries_same_cut` | 22 | 8 |
| `installed_append_binding` | 39 | 6 |
| `certified_checkpoint` | 5 | 3 |

The cursor controls include a substituted hold extent, a failed delivery that advances, and a completion from the wrong predecessor.
The composition law checks that failed delivery leaves the exact original cut and that successful retry advances once.
The importer rejected substitution of the recorded cut, detached sequence, digest, key generation, and append coordinates.

Lean 4.35.0 checked `RTDB.Recovery.cursor_replay_covers_exact_segment` and `RTDB.Recovery.certified_checkpoint_with_retried_delivery`, as well as their `AcknowledgedRecovery` and `AcknowledgedRecoveryControls` importers.
Both new theorems use only `propext` and `Quot.sound`.

TLC 2.19 exhausted 260,916 distinct states modulo keeper permutations in the safety model with two writes, two writer replacements and one lost keeper.
The smaller fair repeated-restart configuration exhausted 52,259 distinct states and satisfied `EventuallyReady`.
The early-trim, partial-sync and lost-quorum controls failed at the expected invariants.
The post-restart read-and-write witness was reached.

The cursor theorem assumes that the authenticated hold and replay page identify the original journal values.
The Lean fold assumes that the installed partition application realizes the supplied event interpretation.
Native runtime compilation, authenticated provider qualification, and complete application-to-promotion correspondence remain unverified.
