#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
: "${TLA2TOOLS_JAR:?Set TLA2TOOLS_JAR to the TLA+ tool jar}"
lean="${LEAN:-lean}"

catalog=dev/doc/rtdb-engine-proof-properties.md
ledger=dev/doc/rtdb-engine-verification-ledger.md
diff -u <(sed -nE 's/^- \*\*([A-Z]+[0-9]{2}) · .*/\1/p' "$catalog") \
        <(sed -nE 's/^\| ([A-Z]+[0-9]{2}) · .*/\1/p' "$ledger")

mkdir -p build/proof-runs
run="$(mktemp -d "$PWD/build/proof-runs/run.XXXXXXXX")"
export LEAN_PATH="$run${LEAN_PATH:+:$LEAN_PATH}"

for proof in dev/lean/*.lean; do
    file="${proof##*/}"
    log="$run/$file.log"
    if ! "$lean" -DwarningAsError=true --root=dev/lean -o "$run/${file%.lean}.olean" "$proof" > "$log" 2>&1; then
        cat "$log" >&2
        exit 1
    fi
    printf '%s checked\n' "$proof"
done

while IFS='|' read -r module config expected_status expected_message; do
    log="$run/$config.log"
    mkdir -p "$run/$config/tmp"
    cp "dev/tla/$config.cfg" "dev/tla/$module.tla" "$run/$config/"
    status=0
    (
        cd "$run/$config"
        java "-Djava.io.tmpdir=$run/$config/tmp" -XX:+UseParallelGC -Xmx1300m \
            -jar "$TLA2TOOLS_JAR" -workers 2 -metadir states \
            -config "$config.cfg" "$module.tla"
    ) > "$log" 2>&1 || status=$?
    if [[ "$status" != "$expected_status" ]] || ! rg -Fq "$expected_message" "$log"; then
        cat "$log" >&2
        exit 1
    fi
    printf '%s: %s\n' "$config" "$expected_message"
done <<'CONFIGURATIONS'
RTDBCellTransfer|RTDBCellTransfer|0|No error has been found
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
CONFIGURATIONS

printf 'Proof logs: %s\n' "$run"
