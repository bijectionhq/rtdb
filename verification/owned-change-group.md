# Held journal group selection

A held merged scan selects the first eligible original transaction position from the current heads of the selected relation journals.
The page can report quiet completion only when no eligible head remains.
Every decoded fragment of a selected transaction must repeat its position and history time, and its event count must extend that relation's previous frontier exactly.

The reader passes the eligible positions in journal order to the checked selector and uses the winning index to read the original header.
The reader checks each stored fragment and stages the whole transaction before adding it to the page.
Sorting by timestamp alone could collapse two publications in the same time band.
Selecting a group from one relation without checking each contributing header and its predecessor could accept an incomplete group.

For a replayable history, enroll after position 18 and give two selected relations their next entries at positions 19 and 20 with the same history time.
Forward selection visits 19 before 20.
If an entry at position 19 repeats position 19 with a substituted history time, or its event count fails to cover its predecessor through the advertised frontier, it cannot contribute to a returned group.
The Verus area includes controls that choose the later position and accept a stored entry with another history time.

The proof's inputs are the eligible heads from the current hold and the decoded headers and stored entries supplied by the Native Node reader.
This decision supplies neither enrollment-time custody nor evidence that the source remained available through reclamation or restore.
[Production caller](../rust/crates/node/src/node/owned_changes.rs) · [Verus laws](verus/owned_change_group/laws.rs)
