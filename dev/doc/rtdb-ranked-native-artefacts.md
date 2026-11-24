# Native ranked artefact readiness

A native v1 declaration and a persisted index serve different contracts.
The applied declaration identifies the index generation selected by the user's basis.
The physical readiness check also needs the exact committed primary catalogue for that database incarnation, branch, layer and basis.

## Spatial build source

On the Default shared-main path, the existing ordered projection stages native spatial definitions in `xt.native_spatial_builds`.
The projection carries the target table, ordered columns, geometry encoding, and native declaration position.
The spatial lane uses its existing funded Arrow reader and posting writer, under the physical trie owner.
Native definitions use a distinct artefact identifier within that owner.
Dropping the table withdraws the projected definition through the same ordered path.
Retained declarations that predate the projection need replay of their ordered declaration or a writer-authorized backfill.

## IVF build and readback

The same ordered projection stages IVF definitions in `xt.native_vector_builds`.
The existing generation writer uses the exact value column, ordered equality partitions, dimensions, recall, and original native declaration position.
Native generations keep a distinct artefact identity under each current trie and retain the main writer's funded component spans and IPC ownership.
For physical readiness the checker requires each selected current trie's persisted generation.
It reopens the immutable object, checks the footer and directory against the recorded locator, and uses the funded decoder to verify every indexed cluster.
The check reports current-trie generation coverage.
The tag 10 catalog query reports the exact SQL method independently of this result.
An explicit [native IVF Query](rtdb-ranked-query.md#native-ivf) rechecks the requested generation at its own held cut and probes the verified bytes from that read.

## Full-text build and readback

The ordered search projection in `xt.native_search_builds` feeds analyzer, history and ordered columns to the existing full-text artefact lane.
Default can prove an exact selected placement for format 13 postings with the recorded owner, file size, authenticated footer and posting checksums.
Other search formats remain false until their owner can prove the equivalent complete coverage.
Logical search needs a proof for the run, forward witness and primary inventory together.

## Readiness dependencies

The native Default read builds its SQL snapshot from the selected branch fold.
Its replica owner pairs the applied native head with the installed physical catalogue at publication.
Readiness admits the current merged main branch when its exact native cut, clock, and physical catalogue agree.
The checker retains that catalogue, compares the complete mirrored definition, and reopens each posting file from storage under its recorded trie owner and exact index generation.
The checker verifies the file length, digest, and Arrow layout within the existing spatial read budget.
On versioned storage the response supplying the bytes must also carry the exact version in the artefact record.
An unversioned record can match an unversioned read under its captured owner and content checks.
Terminal publications need a same-read terminal observation before they can pass this check.
An empty persisted inventory, a live tail, missing or invalid objects, unmatched cuts, and routes without this association report false.

## Multi prerequisites

`MultiDatabase::fresh_view` supplies a committed `BranchIndex` and key generation, from which `MultiServing::query` constructs a live branch snapshot.
The native Multi checkpoint manifest doesn't supply the selected primary trie catalogue and its artefact inventory to that Query path.
The Multi writer needs to project the ordered native IVF declaration into its own persisted build catalogue and publish the primary tries and their funded IVF generations under the deployment's fenced writer and storage generation.
The committed publication must bind the deployment incarnation, branch cut and clock, exact primary ownership, and per-trie generation addresses, lengths, locators and provider versions.
The Multi reader needs an exact-cut capture of that publication, the matching storage-generation grant, and a retained catalogue read that covers primary resolution, artefact probing and delivery under Multi's retention and key authority.
Until those dependencies are installed, Multi serves the exact branch-fold query and omits `query.ranked-ivf`.

## Reading the catalogue

Negotiate `query.ranked-artefact-readiness` and set `verify_ranked_artefacts = true` (QueryRequest field 10) along with the clear exact-cut guard and tracking.
Read `xt.ranked_indexes` without a Flight continuation.
The row reports the declared options and original generation, the result of the physical proof in `artefacts_ready`, and the permitted method in `serving_mode`.
Check the declaration at its original receipt; check physical readiness at a current exact basis once the writer has published the postings.
This catalog query reports `serving_mode = 'exact-sql'` and `approximate = false`.
Persisted postings require a mounted exact-cut spatial selector before the query can use them for narrowing.
