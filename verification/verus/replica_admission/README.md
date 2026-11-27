# R2 · Fenced term admission

The harness imports the production `TermFence::admit` and `claim_term`, the driver's pure promotion guards, and the real signed term and sequence types.
The harness erases the diagnostic payload of `Anomaly::fault` and retains its `Err` outcome on counter overflow.

| Verus law | Lean reference in [FencedTermAdmission](../../multi/lean/FencedTermAdmission.lean) | Runtime use |
| --- | --- | --- |
| `a_superseded_record_is_fenced` | `fences_superseded`, `a_superseded_record_is_fenced` | `TermFence::admit` returns `Fenced` and preserves the term and sequence position on a lower term. |
| `admission_preserves_the_high_water_mark` | `highest_after_admission`, `a_superseded_term_stays_fenced` | The successful fold takes the maximum of the current and arriving term, and an error preserves the fence. |
| `fenced_promotion_after_empty_read` | `fenced_promotion`, `may_promote`, `fenced_promotion_after_empty_read` | `ReplicationDriver::lead_after_empty_read` derives the count from the returned poll and checks the recognized and applied prefix, permission and the grant, then asks the fence for the next term. `promote_term` initializes the appender at sequence zero. |
| `fenced_driver_never_starts_a_claim` | `should_start_claim`, `fenced_driver_never_starts_a_claim` | `ReplicationDriver::start_claim` refuses the fenced mode before creating a claim. |

The abstraction maps a `Term` to its signed integer, the prepared-head count to zero or one, a legacy batch to its length, and the grant predicate to the Boolean returned by the journal.
A checked term overflow maps to no promotion.
The Lean reachability witness promotes from term zero and subsequently fences a term-zero delivery.
Term admission is relative to the highest term observed at read-back.
The driver supplies a positive grant observation from one point in time.
The journal enforces the current grant on each append, including after a grant changes between observation and append.
The asynchronous tail, grant lifetime, driver wiring and ordered application of already-admitted buffered records are outside the Verus laws.
