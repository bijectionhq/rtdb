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

## Readiness dependencies

The native Default read currently builds its SQL snapshot from the selected branch fold.
The Default replica publishes its physical catalogue at a separate replica coordinate after applying the shared-main projection.
A physical readiness proof needs the replica owner's association between that published cut and the exact applied branch state.
The reader must retain the selected catalogue and confirm the live primary inventory and the exact generation's persisted posting files in that placement.

Multi's committed view returns the native fold and key generation, and the native query snapshot constructs its own live rows.
Multi needs a committed primary catalogue and an artefact publication under its existing writer and storage authority before the same spatial proof can be made.
The reader needs the associated storage generation and a retained read on the selected catalogue.

The current native spatial method remains exact SQL.
Published postings require a mounted exact-cut spatial selector before a read can use them for narrowing.
