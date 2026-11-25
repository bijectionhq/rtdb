# Shared plan cache

The node's shared plan cache serves a plan built in one session to another.
The area verifies the shipped decisions it rests on, and [`SharedPlans.lean`](../../lean/SharedPlans.lean) proves the cache laws over an abstract model of planning.

## Verus

The harness imports [`shared_plan_entries.rs`](../../../rust/crates/query/src/shared_plan_entries.rs), which holds the cache's entry operations, and [`lookup_replay.rs`](../../../rust/crates/plan/src/lookup_replay.rs), the loop behind `CatalogLookups::replay`.

| Function | Checked behaviour |
| --- | --- |
| `find` | Returns an index only for an entry whose key equals the requested key, and none when no entry's key does. |
| `replays_every` | Accepts only when every recorded lookup's comparison answered the same. |
| `admit` | Leaves at most `capacity` entries with pairwise distinct keys, the admitted entry last, and every other entry one that was already held. |
| `evict_oldest`, `touch` | Remove exactly the least recently used entry, and move a used entry to the back without losing one. |

`SharedPlanKey` stands for the production key, whose derived equality compares the SQL source and every `PrepareOptions` field.
The harness's catalog in `a_replayed_plan_saw_every_recorded_answer` is a vector of answers; the production comparison is the `CatalogLookups::replay` closure.
Budget accounting is the `MemoryBudget` reservation each entry holds, outside this area; the Lean model states its law.

## Lean

`SharedPlans.lean` models a planner as a decision tree over catalog questions, so later questions may depend on earlier answers.
It proves that a catalog giving every recorded answer leads planning to the same plan, and over a cache of entries, lookups, admissions, evictions and removals:

- a plan served from the cache equals the plan re-planning produces (`a_cached_plan_is_the_fresh_plan`, `every_request_is_served_the_fresh_plan`);
- an entry whose recorded answer the catalog has changed is never served (`a_stale_plan_is_never_served`);
- EXPLAIN and DML requests are planned afresh and leave the cache unchanged (`explain_and_dml_bypass_the_cache`);
- every reachable cache holds reservations equal to its entries' bytes, within the budget and the capacity, and evicting every entry releases every reservation (`every_reachable_cache_keeps_the_budget`, `evicting_every_entry_releases_every_reservation`).

The model's premise is that planning reads only its key and its catalog answers.
`rtdb_sql::prepare_in` and `PreparedQuery::from_prepared` take only the source, `PrepareOptions`, a `Catalog` and a memory budget, and `shared_plan` keys on the source and every `PrepareOptions` field by exhaustive destructuring.

## Controls

`area.json` declares five source mutations: a lookup that returns an entry with another key, replay that accepts a changed answer, admission that keeps an equal key's entry or exceeds the capacity, and use that drops the entry.
`verify_shared_plans_lean` in the runner rejects four model mutations: serving without replay, admitting past the budget, evicting without releasing, and EXPLAIN entering the cache.
