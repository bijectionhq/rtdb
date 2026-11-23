#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/../.."
: "${TLA2TOOLS_JAR:?Set TLA2TOOLS_JAR to the TLA+ tool jar}"
case "$TLA2TOOLS_JAR" in /*) ;; *) TLA2TOOLS_JAR="$PWD/$TLA2TOOLS_JAR" ;; esac
lean="${LEAN:-lean}"

catalog=dev/doc/rtdb-engine-proof-properties.md
ledger=dev/doc/rtdb-engine-verification-ledger.md
diff -u <(sed -nE 's/^- \*\*([A-Z]+[0-9]{2}) · .*/\1/p' "$catalog") \
        <(sed -nE 's/^\| ([A-Z]+[0-9]{2}) · .*/\1/p' "$ledger")

mkdir -p build/proof-runs/multi
run="$(mktemp -d "$PWD/build/proof-runs/multi/run.XXXXXXXX")"
export LEAN_PATH="$run${LEAN_PATH:+:$LEAN_PATH}"

for proof in verification/multi/lean/*.lean; do
    file="${proof##*/}"
    log="$run/$file.log"
    if ! "$lean" -DwarningAsError=true --root=verification/multi/lean -o "$run/${file%.lean}.olean" "$proof" > "$log" 2>&1; then
        cat "$log" >&2
        exit 1
    fi
    printf '%s checked\n' "$proof"
done

while IFS='|' read -r module config expected_status expected_message; do
    log="$run/$config.log"
    mkdir -p "$run/$config/tmp"
    cp "verification/multi/tla/$config.cfg" "verification/multi/tla/$module.tla" "$run/$config/"
    status=0
    (
        cd "$run/$config"
        java "-Djava.io.tmpdir=$run/$config/tmp" -XX:+UseParallelGC -Xmx1300m \
            -jar "$TLA2TOOLS_JAR" -noGenerateSpecTE -workers 2 -metadir states \
            -config "$config.cfg" "$module.tla"
    ) > "$log" 2>&1 || status=$?
    if [[ "$status" != "$expected_status" ]] || ! rg -Fq "$expected_message" "$log"; then
        cat "$log" >&2
        exit 1
    fi
    printf '%s: %s\n' "$config" "$expected_message"
done <<'CONFIGURATIONS'
RTDBCellTransfer|RTDBCellTransfer|0|No error has been found
RTDBCellTransfer|RTDBCellTransferWithoutFrames|12|Invariant RoutedCheckpointFramed is violated
RTDBDeletion|RTDBDeletion|0|No error has been found
RTDBDeletion|RTDBDeletionUndrainedAdministrator|12|Invariant NoObjectAfterReceipt is violated
RTDBDeletion|RTDBDeletionWithoutLegacyHorizon|12|Invariant FirstInventoryFollowsExpiry is violated
RTDBDeletion|RTDBDeletionWithoutSpacing|12|Invariant NoObjectAfterReceipt is violated
RTDBRecovery|RTDBRecovery|0|No error has been found
RTDBRecovery|RTDBRecoveryEarlyTrim|12|Invariant TrimFollowsPublication is violated
RTDBRecovery|RTDBRecoveryLostQuorum|12|Invariant AcknowledgedWritesSurvive is violated
RTDBRecovery|RTDBRecoveryPartialSync|12|Invariant SyncedHistoryIsComplete is violated
RTDBRecovery|RTDBRecoveryRepeatedRestart|0|No error has been found
RTDBRecovery|RTDBRecoveryWitness|12|Invariant NoPostRestartReadAndWrite is violated
RTDBRecoveryIdentity|RTDBRecoveryIdentity|0|No error has been found
RTDBRecoveryIdentity|RTDBRecoveryIdentityReuse|12|Invariant RecoveredHistoryPreservesAcknowledgedWrite is violated
RTDBRecoveryIdentity|RTDBRecoveryIdentityStartup|12|Invariant FailedJournalsAreIneligible is violated
RTDBJournalReplacement|RTDBJournalReplacement|0|No error has been found
RTDBJournalReplacement|RTDBJournalReplacementPartialCopy|12|Invariant AcknowledgedPrefixRecoverable is violated
RTDBJournalReplacement|RTDBJournalReplacementUnpublishedTrim|12|Invariant AcknowledgedPrefixRecoverable is violated
RTDBCatalogReaderBatch|RTDBCatalogReaderBatch|0|No error has been found
RTDBCatalogReaderBatch|RTDBCatalogReaderBatchStaleVersion|12|Invariant AdmissionsRespectFloor is violated
RTDBCatalogReaderBatch|RTDBCatalogReaderBatchAbsentRelease|12|Invariant ReleaseFencesAdmission is violated
RTDBPlacedOpen|RTDBPlacedOpen|0|No error has been found
RTDBPlacedOpen|RTDBPlacedOpenSerial|13|Temporal property IndependentEventuallyOpens was violated
RTDBPlacedOpen|RTDBPlacedOpenAfterShutdown|12|Invariant ShutdownFencesPublication is violated
RTDBGatewayReplay|RTDBGatewayReplay|0|No error has been found
RTDBGatewayReplay|RTDBGatewayReplaySqlKey|12|Invariant OneEffect is violated
RTDBGatewayReplay|RTDBGatewayReplayMalformedKey|12|Invariant OneEffect is violated
RTDBGatewayReplay|RTDBGatewayReplayRenewedWindow|12|Invariant FixedCutoff is violated
RTDBServingFailure|RTDBServingFailure|0|No error has been found
RTDBServingFailure|RTDBServingFailureControlOnly|13|Temporal property StartupEventuallyConnects was violated
RTDBServingFailure|RTDBServingFailureStaleProbe|12|Invariant EvidenceMatchesCurrentEndpoints is violated
RTDBServingFailure|RTDBServingFailureOneDial|13|Temporal property StartupEventuallyConnects was violated
RTDBGatewayPaths|RTDBGatewayPaths|0|No error has been found
RTDBGatewayPaths|RTDBGatewayPathsTcpOnly|13|Temporal property EligibleStartupCompletes was violated
RTDBGatewayPaths|RTDBGatewayPathsDirectOnly|13|Temporal property EligibleStartupCompletes was violated
RTDBGatewayPaths|RTDBGatewayPathsChangedAuth|12|Invariant RelayPreservesAuthentication is violated
RTDBGatewayPaths|RTDBGatewayPathsCrowded|12|Invariant CrowdedProbeKeepsWriter is violated
RTDBGatewayPaths|RTDBGatewayPathsOverflowFirst|13|Temporal property EligibleStartupCompletes was violated
RTDBReplayPermit|RTDBReplayPermit|0|No error has been found
RTDBReplayPermit|RTDBReplayPermitBatch|12|Invariant OneMutation is violated
RTDBReplayPermit|RTDBReplayPermitDelayedOriginal|12|Invariant OneMutation is violated
RTDBReplayPermit|RTDBReplayPermitEarlyExpiry|12|Invariant OneMutation is violated
RTDBReplayPermit|RTDBReplayPermitErasedMarker|12|Invariant OneMutation is violated
RTDBReplayPermit|RTDBReplayPermitLegacyWriter|12|Invariant OneMutation is violated
RTDBReplayResolver|RTDBReplayResolver|0|No error has been found
RTDBReplayResolver|RTDBReplayResolverWithoutAssertion|12|Invariant OneEffect is violated
RTDBSetupReplay|RTDBSetupReplay|0|No error has been found
RTDBSetupReplay|RTDBSetupReplayStaleCheck|12|Invariant SetupReplayHasNoExecute is violated
CONFIGURATIONS

printf 'Proof logs: %s\n' "$run"
