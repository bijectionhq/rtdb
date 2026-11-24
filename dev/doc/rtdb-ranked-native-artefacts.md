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

## Multi publication and mount

The Multi writer selects a settled merged-main basis and its ordered IVF declarations.
Maintenance waits for a stable cut before staging primary level-zero tries and their IVF files with the common Arrow writers.
The writer seals each file into its own allocation at the selected storage key generation and commits the complete manifest under its journal epoch.
The publication binds the database incarnation, basis, system clock, declaration generations, primary owners, lengths, IVF locators and provider versions.
The journal retains that root with its files until trim.
Reopening an existing incarnation can recover the publication from the journal.
Restore into a fresh incarnation rebuilds the physical catalogue from the restored fold and publishes under the destination's own epoch and key generation.

An unscoped exact merged-main read mounts only a publication for its selected cut and current key generation.
The query retains the authenticated primary and IVF bytes in a bounded read region through execution and delivery.
Readiness validates the primary file layout and every current-trie IVF generation for the selected declaration.
An IVF Query uses those primary tries to resolve the probed candidates at the same cut.
Delivery checks the selected key generation and the serving attachment.
An assigned follower mounts the publication from its locally applied prefix, and optional follower progress describes the same selected cut.
Unpublished cuts, empty primary inventories and unreadable, mismatched or unsupported publications report false for readiness and refuse explicit IVF.
The readiness row still describes exact SQL independently of the separately negotiated IVF execution.

## Reading the catalogue

Negotiate `query.ranked-artefact-readiness` and set `verify_ranked_artefacts = true` (QueryRequest field 10) along with the clear exact-cut guard and tracking.
Read `xt.ranked_indexes` without a Flight continuation.
The row reports the declared options and original generation, the result of the physical proof in `artefacts_ready`, and the permitted method in `serving_mode`.
Check the declaration at its original receipt; check physical readiness at a current exact basis once the writer has published the postings.
This catalog query reports `serving_mode = 'exact-sql'` and `approximate = false`.
Persisted postings require a mounted exact-cut spatial selector before the query can use them for narrowing.
