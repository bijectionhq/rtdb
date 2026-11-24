# Prepared Landing basis Verus execution

Source commit: `76583779346b7aa4cb4dbf8617616c272e660338`.
Verifier: Verus 0.2026.11.05.31e44e6, darwin/arm64, Rust 1.99.1.
Owner authorization: `BIJECTION_LOCAL_EXECUTION=owner-authorized`.

Ran `verification/run.py::verify_verus` for the `prepared_landing_basis` area with the pinned Verus and its Z3 binary.
The runner used `--output-json --multiple-errors 5 --no-external-by-default` on the baseline and each manifest mutation in a separate source snapshot.

| Case | Verified functions | Expected diagnostics |
| --- | ---: | --- |
| Baseline, including `laws::switch_preserves_prepared_basis` | 4 | 0 |
| `substitute-predecessor` | 3 | 1 postcondition failure in `generation_matches` |
| `substitute-replica-epoch` | 3 | 1 postcondition failure in `generation_matches` |
| `substitute-assigned-position` | 3 | 1 postcondition failure in `generation_matches` |
| `substitute-assigned-time` | 3 | 1 postcondition failure in `generation_matches` |

The production basis decision is included by path.
The resolver's async choice, Arrow codecs, storage recovery, and production crate compilation are outside this run.
No RTDB build or test suite was run.
