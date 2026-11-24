# RTDB consumer contract v1 proof record

Proof input commit `0489dad3a35f70c5076575f05f979d2e6c608360`.
Contract SHA-256 `80c284d4a02f90f44b3e9fe8a7b1468207a8ea2b977a6013ea5bed6750bcac99`.
The former published clauses and proof mapping are in commit `4f2c8cab4c` under `verification/contracts/`.

`verification/check_consumer_contract.py` ran with Verus 0.2026.11.05.31e44e6, Rust 1.99.1, and Lean 4.24.0.
Verus checked each named original function and rejected every declared mutant at its expected diagnostic.

| Verus area | Baseline functions verified | Controls matched |
| --- | ---: | ---: |
| held_worlds_order | 38 | 10 |
| owned_archive_files | 12 | 2 |
| owned_change_group | 5 | 2 |
| owned_landing_wire | 19 | 4 |
| prepared_landing_basis | 7 | 5 |

Lean checked OrderedCapacity, OrderedJournal, and HeldWorldsOrder with warnings treated as errors.
The four named held-worlds model exports use only propext and Quot.sound.
Production Rust was compiled only where included by Verus.
Production builds and test suites were not run.
