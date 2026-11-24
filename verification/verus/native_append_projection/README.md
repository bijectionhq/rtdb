# Native append projection

[`NativeAppend`](../../multi/compose/NativeAppend.lean) defines the PX1 mapping in two steps.
`nativeAppend` constructs `Logkeeper.Protocol.Append` from the chosen journal record's request identity, request fingerprint, outcome identity, exclusive cut, retry expiry and exact debit.
The identity uses the installed placement, authenticated issuer and request nonce.
The outcome identity names the exact ordered `AppendResult`.
Its data cut uses `result.cut.next_lsn` and `result.cut.history`.
The attempt expires at the invocation's decision-position horizon.
The charge class follows `result.class` and the byte charge follows `result.byte_end - result.byte_start`.
The original record LSN is the exclusive frontier minus one.
The term sequence, data LSN and command position are distinct coordinates.

The installed `JournalLog::enqueue_at` seals the replica frame using the writer grant's generation and places those bytes in `Command::Append`.
`finish_append` checks the chosen result's class and assigned LSN.
`Cursor::deliver` opens the chosen payload using the recorded grant's generation before the replication driver decodes it.
The separate PR12 evidence links acknowledgement to exact read-back and successful apply.

`decodeFrame` models `decode_replica_frame` in the shipped decoder.
The Verus law `decode_chosen_frame` preserves the whole Prost body, term default and optional assigned sequence on success, and characterizes rejection under the registered `prost-log-message` contract.
`decoded_acknowledgement_preserves_body` checks that the shipped `ReplicaFrame::into_envelope` passes the complete recognized body through.
`decodedEvent` projects a resolved transaction from that body and `decodeAppend` applies this projection to the frame bound to the chosen append.
A control or an unopened oversized payload yields no transaction event.

[Installed append binding](../installed_append_binding/README.md) checks selected ciphertext, recorded key identity and detached-frame validation.
`DecodeSource.openedFrame` and `resolveDetached` state those operations in Lean.
The verified projection uses the existing Prost and stage-check leaves.
The provider and authenticated-opening contracts supply the bytes, and the Arrow and logical effect decoder in `rtdb_tx::decode_replica` remains a separate refinement.
