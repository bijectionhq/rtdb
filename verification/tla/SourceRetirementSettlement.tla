------------------------ MODULE SourceRetirementSettlement --------------------------
EXTENDS Integers

CONSTANTS
    \* @type: Str;
    Design,
    \* @type: Str;
    Mutation

VARIABLES
    \* @type: Int;
    current,
    \* @type: Set(Int);
    resident,
    \* @type: Int;
    held,
    \* @type: Bool;
    authorized,
    \* @type: Bool;
    pending,
    \* @type: Bool;
    stopped,
    \* @type: Bool;
    settled,
    \* @type: Bool;
    completedAfterStop

\* @type: <<Int, Set(Int), Int, Bool, Bool, Bool, Bool, Bool>>;
Vars == <<current, resident, held, authorized, pending, stopped, settled, completedAfterStop>>

\* One key, two immutable physical versions, one retireable old version.
Init ==
    /\ current = 1 /\ resident = {1, 2} /\ held = 0
    /\ authorized = FALSE /\ pending = FALSE /\ stopped = FALSE
    /\ settled = FALSE /\ completedAfterStop = FALSE

Authorize ==
    /\ ~authorized /\ ~settled /\ ~stopped /\ current = 1 /\ held # 1
    /\ authorized' = TRUE
    /\ UNCHANGED <<current, resident, held, pending, stopped, settled, completedAfterStop>>

Submit ==
    /\ authorized /\ ~pending /\ ~stopped
    /\ pending' = TRUE
    /\ UNCHANGED <<current, resident, held, authorized, stopped, settled, completedAfterStop>>

Stop ==
    /\ ~stopped /\ stopped' = TRUE
    /\ UNCHANGED <<current, resident, held, authorized, pending, settled, completedAfterStop>>

PublishReplacement ==
    /\ current = 1 /\ 2 \in resident
    /\ Design = "exact" \/ ~authorized
    /\ current' = 2
    /\ UNCHANGED <<resident, held, authorized, pending, stopped, settled, completedAfterStop>>

AdmitReader ==
    /\ held = 0 /\ current \in resident
    /\ ~authorized \/ (Design = "exact" /\ current = 2)
    /\ held' = current
    /\ UNCHANGED <<current, resident, authorized, pending, stopped, settled, completedAfterStop>>

DrainReader ==
    /\ held # 0 /\ held' = 0
    /\ UNCHANGED <<current, resident, authorized, pending, stopped, settled, completedAfterStop>>

Remove ==
    /\ pending /\ pending' = FALSE
    /\ LET target == IF Design = "exact" /\ Mutation # "delete_by_current_path" THEN 1 ELSE current
       IN resident' = resident \ {target}
    /\ completedAfterStop' = (completedAfterStop \/ (stopped /\ held = 2))
    /\ UNCHANGED <<current, held, authorized, stopped, settled>>

Settle ==
    /\ authorized
    /\ (1 \notin resident /\ ~pending) \/ (stopped /\ Mutation = "task_stop_settles_io")
    /\ authorized' = FALSE /\ settled' = TRUE
    /\ UNCHANGED <<current, resident, held, pending, stopped, completedAfterStop>>

Next == Authorize \/ Submit \/ Stop \/ PublishReplacement \/ AdmitReader \/ DrainReader \/ Remove \/ Settle
Spec == Init /\ [][Next]_Vars

ReaderRemainsCovered == held # 0 => held \in resident
ReplacementSurvives == current = 2 => 2 \in resident
SubmissionStaysAuthorized == pending => authorized
SettlementMeansOldVersionAbsent == settled => 1 \notin resident /\ ~pending
Safe == ReaderRemainsCovered /\ ReplacementSurvives /\ SubmissionStaysAuthorized /\ SettlementMeansOldVersionAbsent

NeverCompletesAfterStopWithReplacementHeld == ~completedAfterStop
NeverSettlesThenAdmitsReplacement == ~(settled /\ held = 2)
======================================================================================
