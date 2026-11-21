------------------------------ MODULE ErasureCleanup ------------------------------
EXTENDS Naturals

CONSTANT ForgetOnAdvance

VARIABLES frontier, pending, oldPage, phase

vars == <<frontier, pending, oldPage, phase>>
Idle == "idle"
Repair == "repair"
Sweep == "sweep"

Init ==
  /\ frontier = 0
  /\ pending = 0
  /\ oldPage = TRUE
  /\ phase = Idle

RequestErase ==
  /\ frontier = 0
  /\ pending = 0
  /\ phase = Idle
  /\ pending' = 1
  /\ UNCHANGED <<frontier, oldPage, phase>>

PublishErasure ==
  /\ frontier = 0
  /\ pending = 1
  /\ phase = Idle
  /\ frontier' = 1
  /\ UNCHANGED <<pending, oldPage, phase>>

Advance ==
  /\ frontier = 1
  /\ phase = Idle
  /\ frontier' = 2
  /\ pending' = IF ForgetOnAdvance THEN 0 ELSE pending
  /\ UNCHANGED <<oldPage, phase>>

StartRepair ==
  /\ frontier > 0
  /\ pending = 0
  /\ phase = Idle
  /\ pending' = frontier
  /\ phase' = Repair
  /\ UNCHANGED <<frontier, oldPage>>

UploadRepairPage ==
  /\ phase = Repair
  /\ oldPage' = TRUE
  /\ phase' = Idle
  /\ UNCHANGED <<frontier, pending>>

Mark ==
  /\ frontier > 0
  /\ pending > 0
  /\ pending <= frontier
  /\ phase = Idle
  /\ phase' = Sweep
  /\ UNCHANGED <<frontier, pending, oldPage>>

DeletePage ==
  /\ phase = Sweep
  /\ oldPage
  /\ oldPage' = FALSE
  /\ UNCHANGED <<frontier, pending, phase>>

Discharge ==
  /\ phase = Sweep
  /\ ~oldPage
  /\ phase' = Idle
  /\ pending' = 0
  /\ UNCHANGED <<frontier, oldPage>>

Crash ==
  /\ phase # Idle
  /\ phase' = Idle
  /\ UNCHANGED <<frontier, pending, oldPage>>

Next ==
  \/ RequestErase \/ PublishErasure \/ Advance \/ StartRepair \/ UploadRepairPage
  \/ Mark \/ DeletePage \/ Discharge \/ Crash

Spec == Init /\ [][Next]_vars

NoLostCleanup == frontier > 0 /\ oldPage => 0 < pending /\ pending <= frontier

=============================================================================
