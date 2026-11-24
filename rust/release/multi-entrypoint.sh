#!/bin/sh
set -eu
umask 077

bind_hosted_task() {
    unset RTDB_ECS_TASK_ARN
    for option in "$@"; do
        if [ "$option" = "--ecs-cluster" ]; then
            hosted_metadata=$(curl --fail --silent --max-time 5 "${ECS_CONTAINER_METADATA_URI_V4:?Missing ECS task metadata}/task")
            RTDB_ECS_TASK_ARN=$(printf '%s' "$hosted_metadata" | jq -er '.TaskARN | select(type == "string" and length > 0)')
            export RTDB_ECS_TASK_ARN
            return
        fi
    done
}

write_replay_client() {
    printf '%s' "${RTDB_REPLAY_BROKER_CA:?Missing replay broker CA}" > replay-ca.pem
    printf '%s' "${RTDB_REPLAY_BROKER_TOKEN:?Missing replay broker token}" > replay.token
    unset RTDB_REPLAY_BROKER_CA RTDB_REPLAY_BROKER_TOKEN
}

case "${1:-}" in
    gateway | node | controller)
        role=$1
        shift
        cd "$(mktemp -d /tmp/rtdb-installation.XXXXXXXX)"
        printf '%s' "${RTDB_CELL_SECRET:?Missing cell secret}" > cell.secret
        if [ -n "${RTDB_INSTALLED_JOURNALS_FILE:-}" ] && [ "$role" != controller ]; then
            set -- --installed-journals "$RTDB_INSTALLED_JOURNALS_FILE" "$@"
        fi
        case "$role" in
            gateway)
                printf '%s' "${RTDB_TLS_KEYSTORE:?Missing TLS keystore}" | base64 --decode > gateway.p12
                printf '%s' "${RTDB_CONFIGURATION:?Missing gateway configuration}" > config.yaml
                set -- --config config.yaml "$@"
                if [ -n "${RTDB_INSTALL_TOKEN:-}" ]; then
                    printf '%s' "$RTDB_INSTALL_TOKEN" > install.token
                    set -- --install-token-file install.token "$@"
                fi
                if [ -n "${RTDB_DISPOSAL_TOKEN:-}" ]; then
                    printf '%s' "$RTDB_DISPOSAL_TOKEN" > disposal.token
                    set -- --disposal-token-file disposal.token "$@"
                fi
                ;;
            node)
                bind_hosted_task "$@"
                printf '%s' "${RTDB_CONFIGURATION:?Missing node configuration}" > config.yaml
                set -- --config config.yaml "$@"
                if [ -n "${RTDB_REPLAY_BROKER_CA:-}" ]; then
                    write_replay_client
                    set -- --replay-broker-ca replay-ca.pem --replay-broker-token-file replay.token "$@"
                fi
                if [ -n "${ECS_CONTAINER_METADATA_URI_V4:-}" ]; then
                    task_metadata=$(curl --fail --silent --max-time 5 "$ECS_CONTAINER_METADATA_URI_V4")
                    address=$(printf '%s' "$task_metadata" | jq -er '[.Networks[] | select(.NetworkMode == "awsvpc") | .IPv4Addresses[]] | if length == 1 then .[0] else error("Expected one task IPv4 address") end')
                    set -- --advertise-host "$address" "$@"
                fi
                ;;
        esac
        unset RTDB_CONFIGURATION RTDB_TLS_KEYSTORE RTDB_INSTALL_TOKEN RTDB_DISPOSAL_TOKEN RTDB_CELL_SECRET
        exec /usr/local/bin/rtdb-multi "$role" --cell-secret cell.secret "$@"
        ;;
    reaper)
        shift
        bind_hosted_task "$@"
        if [ -n "${RTDB_RECOVERY_TOKEN:-}" ]; then
            cd "$(mktemp -d /tmp/rtdb-recovery.XXXXXXXX)"
            printf '%s' "${RTDB_TLS_KEYSTORE:?Missing recovery TLS keystore}" | base64 --decode > recovery.p12
            printf '%s' "$RTDB_RECOVERY_TOKEN" > recovery.token
            printf '%s' "${RTDB_CONFIGURATION:?Missing recovery configuration}" > config.yaml
            set -- --recovery-config config.yaml --recovery-token-file recovery.token "$@"
            if [ -n "${RTDB_CELL_SECRET:-}" ]; then
                printf '%s' "$RTDB_CELL_SECRET" > cell.secret
                set -- --cell-secret cell.secret "$@"
            fi
            if [ -n "${RTDB_REPLAY_BROKER_CA:-}" ]; then
                write_replay_client
                printf '%s' "${RTDB_REPLAY_TRANSFER_TOKEN:?Missing replay transfer token}" > replay-transfer.token
                set -- --replay-broker-ca replay-ca.pem --replay-broker-token-file replay.token \
                    --replay-broker-transfer-token-file replay-transfer.token "$@"
            fi
        fi
        unset RTDB_CONFIGURATION RTDB_TLS_KEYSTORE RTDB_RECOVERY_TOKEN RTDB_CELL_SECRET RTDB_REPLAY_TRANSFER_TOKEN
        exec /usr/local/bin/rtdb-multi reaper "$@"
        ;;
    replay-broker)
        shift
        bind_hosted_task "$@"
        cd "$(mktemp -d /tmp/rtdb-replay-broker.XXXXXXXX)"
        printf '%s' "${RTDB_TLS_KEYSTORE:?Missing replay broker TLS keystore}" | base64 --decode > replay-broker.p12
        printf '%s' "${RTDB_CONFIGURATION:?Missing replay broker configuration}" > config.yaml
        printf '%s' "${RTDB_REPLAY_BROKER_TOKEN:?Missing replay broker token}" > replay.token
        printf '%s' "${RTDB_REPLAY_TRANSFER_TOKEN:?Missing replay transfer token}" > replay-transfer.token
        unset RTDB_CONFIGURATION RTDB_TLS_KEYSTORE RTDB_REPLAY_BROKER_TOKEN RTDB_REPLAY_TRANSFER_TOKEN
        exec /usr/local/bin/rtdb-multi replay-broker --config config.yaml --token-file replay.token \
            --transfer-token-file replay-transfer.token "$@"
        ;;
    prepare-cell-transfer | activate-cell-transfer | capture | restore | stage-checkpoint | read-checkpoint-stage)
        bind_hosted_task "$@"
        exec /usr/local/bin/rtdb-multi "$@"
        ;;
    *)
        exec /usr/local/bin/rtdb-multi "$@"
        ;;
esac
