# W15 integration handoff

## Branch and dependencies

Merge `211316585c..ideal/w15-requires` into the integration worker's `ideal/final`.
The launch base already includes W14's transaction pipeline, W9's claim verification, and W13's scoped serving.
The branch can merge alongside the integration worker's fixes to Put valid-time decoding and the transaction benchmark fixture.
Bijection W10b already vendors the `TransactRequest.requires` field at tag 7.
After both tips are integrated, qualify W10b's ownership-fence calls against a server that advertises `transact.requires`.

## Transaction behavior

Both write drivers pass requirements to the shared resolver, which evaluates them in request order before branch guards.
The first false requirement yields `Refused` with conflict code `rtdb/requirement-failed` and the message `Requirement <zero-based index> failed`.
A request with an operation in either arm records that receipt with no row or catalog effects, and retries and `TxStatus` return the same outcome at the stored basis.
An omitted or empty `requires` preserves the request digest of pre-W15 clients.
Multi verifies claims from both lists and interprets scoped `Version::Written` selectors against the acknowledged visibility projection.

`CountAtMost` in `requires` uses `WHEN_BEFORE` because arm selection has yet to occur.
`WHEN_AFTER` remains available in `guards`.
A request with both arms empty remains a read-only validation at a fresh basis under DESIGN §6.
Its refusal is returned inline and leaves no `TxStatus` entry.

## Integration issues and verification

Multi's existing `Transact` admission rejects `Erase` until the qualified key-rotation path is available to that verb.
W10b's guarded TTL sweep still needs that integration for a request carrying `Erase`.
No new dependency or record-version change is required by W15.
The first compiler exposure is expected to be Prost's regenerated `TransactRequest` and any new struct literal introduced by a concurrently merged branch.
Review `Transact` and `TransactRequest` literals when merging another tip that adds call sites.

Builds, tests, benchmarks, compiler checks, and generators were deferred to final qualification.
Run these from the integrated worktree's `rust/` directory for focused verification.

```sh
/Users/sachaichbiah/code/codesign-agents/guarded-cargo.py rtdb test --locked -p rtdb-tx --lib requires_
/Users/sachaichbiah/code/codesign-agents/guarded-cargo.py rtdb test --locked -p rtdb-db --lib multi_driver_requires_refuses_a_displaced_owner_and_recovers_the_receipt
/Users/sachaichbiah/code/codesign-agents/guarded-cargo.py rtdb test --locked -p rtdb-node --lib multi_serving_scoped_requires_preserves_projected_coordinates
/Users/sachaichbiah/code/codesign-agents/guarded-cargo.py rtdb test --locked -p rtdb-flight-sql --test flight rtdb_v1_native_driver_requires_fences_both_arms_and_replays
```
