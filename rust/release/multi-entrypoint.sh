#!/bin/sh
set -eu
umask 077

case "${1:-}" in
    gateway | node | controller)
        role=$1
        shift
        cd "$(mktemp -d /tmp/rtdb-installation.XXXXXXXX)"
        printf '%s' "${RTDB_CELL_SECRET:?Missing cell secret}" > cell.secret
        case "$role" in
            gateway)
                printf '%s' "${RTDB_TLS_KEYSTORE:?Missing TLS keystore}" | base64 --decode > gateway.p12
                printf '%s' "${RTDB_INSTALL_TOKEN:?Missing installation token}" > install.token
                printf '%s' "${RTDB_CONFIGURATION:?Missing gateway configuration}" > config.yaml
                set -- --config config.yaml --install-token-file install.token "$@"
                if [ -n "${RTDB_DISPOSAL_TOKEN:-}" ]; then
                    printf '%s' "$RTDB_DISPOSAL_TOKEN" > disposal.token
                    set -- --disposal-token-file disposal.token "$@"
                fi
                ;;
            node)
                printf '%s' "${RTDB_CONFIGURATION:?Missing node configuration}" > config.yaml
                set -- --config config.yaml "$@"
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
    *)
        exec /usr/local/bin/rtdb-multi "$@"
        ;;
esac
