# Installed journal adapter contract (X3)

RTDB's Rust workspace declares Logkeeper at `1e405da614dc457e924aabfe54a44e764f4447bd`.
The composition package imports the same revision.
The local Lean reference below states the admission contract for the declared Rust pin.

## Admission at the Logkeeper decision

| Request | Pinned Logkeeper admission contract |
| --- | --- |
| All five commands | The exact placement and authenticated principal bind the request. A fresh decision needs a nonzero nonce, an exclusive expiry within the allowed ordered retry window, and an unsealed cohort. Exact retained mutations may reissue their outcome. A barrier's spent request identity conflicts. [Request identity and reducer](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L166-L192) · [Common admission](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L795-L834) |
| Barrier | The authenticated worker principal is the exact current enrollment in the reader roster. The fresh hold uses that invocation and the head at the decision. Hold admission remains subject to capacity. [Barrier](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1174-L1183) |
| ReleaseRead | The worker supplies the original barrier's whole invocation ID (nonce and retry horizon) under the same principal. An existing hold must be ordinary and have the barrier receipt as provenance. An absent hold allows release unless its receipt identifies a mutation. [ReleaseRead](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1441-L1473) |
| Append | The same current-grant guard applies, and `expected_next` equals the current head's next LSN. Admission also checks capacity, record charge and overflow. [Append](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1102-L1139) |
| Lease | The same current-grant guard applies and the exact grant enrollment must still be in the roster. The lease end must fit in journal time. [Lease](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1324-L1341) |
| DisposeExpired | The principal is the custodian or administrator, the exact enrollment serial was issued and is nonzero, and that enrollment has left the roster. The chosen journal time has reached the enrollment's disposal eligibility. [DisposeExpired](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1305-L1323) · [Eligibility](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1626-L1631) |

At the declared Rust pin an append must supply a complete reference list whenever object fences can exist.
An explicit list must contain at most 16,384 strictly ordered nonzero object identities and exclude fenced names.
Logkeeper checks canonicality and fences from the supplied names, and RTDB owes semantic completeness for replay.
The opaque form is admissible only while no object fence exists.
The Lean contract uses natural-number identities with zero reserved and states the semantic completeness obligation over every required replay dependency.
[`Append`](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1102-L1116) · [`admit_object_references`](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1516-L1529) · [Reference shape](https://github.com/bijectionhq/logkeeper/blob/1e405da614dc457e924aabfe54a44e764f4447bd/src/ordered.rs#L1756-L1760)

## Checked adapter decisions

The harness imports the shipped request builders and identity predicates.
It represents each UUID by its full unsigned 128-bit value, each enrollment by worker and serial, each grant by enrollment, epoch and key generation, and each invocation by nonce and exclusive expiry.
The placement contains deployment, incarnation, group and generation.
The stand-in types mirror the pinned fields and variants used by these functions.
Cryptographic verification, TLS authorization, UUID freshness, receipt capacity, serialization and the ordered state machine remain upstream obligations.

| Shipped Rust decision | [Lean reference](../../multi/lean/InstalledJournalGuards.lean) |
| --- | --- |
| `placement_matches`, `scoped_request` | `exactScope` |
| `same_writer`, `lease_command` | `currentGrant` and `writer_request_admissible_under_unchanged_grant` |
| `barrier_command`, `scoped_request` | `barrierReader` and `barrier_names_the_enrolled_reader` |
| `barrier_read_matches`, `release_read_command` | `exactReadIdentity` and `release_names_the_barrier_invocation` |
| `append_command` | `appendIdentity` and `append_request_names_observed_head` |
| `disposal_is_due`, `dispose_expired_command` | `disposalDue`, `expiryDisposalAdmits` and `eligible_withdrawn_enrollment_admits_disposal` |
| `append_command` reference list and opaque-reference counterexample | `appendReferences`, `append_preserves_reference_list`, `referencePresence`, and `opaque_append_is_refused_after_a_fence` |

Barrier authenticates the installed worker enrollment.
Append uses the exact observed writer grant and the locally tracked head only after the entire observed cut matches it.
The caller records the exact fresh barrier invocation before sending and passes it to `ReleaseRead` on uncertain outcomes.
Serving lease renewal uses the saved grant and can send after a route observation already shows a replacement.
The automatic custodian uses the enrollment from Logkeeper's withdrawn-owner page and attempts disposal when either its clock or the observed journal time reaches the reported eligibility.
The standalone administrative disposal command accepts an operator-supplied enrollment.

Admission is conditional on journal state at ordering.
A writer replacement between observation and ordering, expired request, advanced head, saturated hold capacity or a custodian clock ahead of chosen journal time can cause a refusal.
The wired call sites and Logkeeper's reducer supply these temporal and authority checks.
Verus checks that the builder passes through the exact optional list and that the opaque case fails the pinned reference-presence predicate in the presence of a fence.
The local serving path passes an explicit list from `AppendReferences::names`.
The S3 serving path passes the opaque form.
The proof takes the list as input, and semantic completeness of the append projection remains a separate obligation.
