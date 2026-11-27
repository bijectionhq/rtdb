# Recovered writer activation

The harness imports the shared replica driver's production promotion and
activation predicates. A frame outside the recognized prefix or an unapplied
read prefix cannot promote a writer or publish an installed prefix. A fenced
writer stays in recovery after an append, until the driver has confirmed the
recognized position, applied every read record, and installed that exact prefix.

The installed replay cursor previously supplied these two laws and controls.
This harness keeps them with the remaining replica driver after journal
retirement. The companion [term admission harness](../replica_admission/README.md)
covers admission and term sequencing.
