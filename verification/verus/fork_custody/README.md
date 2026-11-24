# R8 · Fork reservation settlement

The verified functions are the two decisions used by `Service::reconcile_domain_forks` before it sends the internal `FinishFork` action.
The harness imports them from the shipped replay broker.

| Verus law | Lean reference in [ForkCustody](../../multi/compose/ForkCustody.lean) | Production inputs |
| --- | --- | --- |
| `retained_preparation_cannot_release` | `mayCheckForkOutcome` | The exact preparation's presence in the source lineage observed by the broker. |
| `release_requires_a_settled_outcome` | `settledForkOutcome` | The child record reread after the preparation disappears. A cleared fork stage is publication evidence. A retained stage with `Deleted` lifecycle is disposal evidence. |

The directory's publication path consumes the exact preparation and publishes the child together.
The broker's dependent reservation can outlive that decision and is removed when reconciliation records ancestry under the source authority or settles a disposed stage.
The Lean model also covers the order of source withdrawal and the independent export branch.
The export transition abstracts completed destination re-encryption of the checkpoint and durable tail writes under destination custody.

The Verus laws start from matching child identity and durable directory observations.
Directory transactions, ancestry authentication, provider and key operations, and end-to-end installed exact-basis copy remain outside the verified release decisions.
