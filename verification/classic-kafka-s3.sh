#!/usr/bin/env bash
set -euo pipefail

: "${RTDB_CLASSIC_BIN:?Set RTDB_CLASSIC_BIN to the absolute path of an rtdb binary built with kafka and s3}"
binary="$RTDB_CLASSIC_BIN"
[[ "$binary" = /* && -x "$binary" ]]
for tool in aws curl docker minio nc psql python3 rg; do command -v "$tool" >/dev/null; done
cd "$(dirname "${BASH_SOURCE[0]}")/.."
mkdir -p build/proof-runs/classic-kafka-s3
proof_run="$(mktemp -d "$PWD/build/proof-runs/classic-kafka-s3/run.XXXXXXXX")"
printf 'Classic recovery fixture logs: %s\n' "$proof_run"
tag="rtdb-proof-$(basename "$proof_run" | tr '[:upper:]' '[:lower:]' | tr . -)"
read -r kafka_port s3_port s3_console pg1 h1 pg2 h2 pg3 h3 pg4 h4 < <(python3 - <<'PY'
import socket

sockets = [socket.socket() for _ in range(11)]
try:
    for listener in sockets:
        listener.bind(("127.0.0.1", 0))
    print(*(listener.getsockname()[1] for listener in sockets))
finally:
    for listener in sockets:
        listener.close()
PY
)
export MINIO_ROOT_USER=rtdb-proof-fixture MINIO_ROOT_PASSWORD=local-test-fixture-only
export AWS_ACCESS_KEY_ID="$MINIO_ROOT_USER" AWS_SECRET_ACCESS_KEY="$MINIO_ROOT_PASSWORD"
unset AWS_SESSION_TOKEN
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_EC2_METADATA_DISABLED=true
export PGSSLMODE=disable PGCONNECT_TIMEOUT=2 PGPASSWORD=rtdb-classic-proof
s3="http://127.0.0.1:$s3_port"
owned_pids=()
kafka_id=""
cleanup() {
    for process in "${owned_pids[@]}"; do
        if jobs -pr | rg -Fxq "$process"; then kill "$process" 2>/dev/null || true; fi
    done
    wait || true
    if [[ -n "$kafka_id" ]]; then docker stop -t 2 "$kafka_id" >/dev/null 2>&1 || true; fi
}
trap cleanup EXIT

minio server --address "127.0.0.1:$s3_port" --console-address "127.0.0.1:$s3_console" "$proof_run/s3" > "$proof_run/minio.log" 2>&1 &
owned_pids+=("$!")
kafka_id="$(docker run -d --rm --name "$tag" -p "127.0.0.1:$kafka_port:$kafka_port" \
    -e KAFKA_NODE_ID=1 -e KAFKA_ENABLE_KRAFT=yes -e KAFKA_PROCESS_ROLES=broker,controller \
    -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e "KAFKA_LISTENERS=PLAINTEXT://:$kafka_port,CONTROLLER://:9093" \
    -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
    -e "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:$kafka_port" \
    -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@127.0.0.1:9093 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1 \
    -e KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1 -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 \
    -e CLUSTER_ID=q1Sh-9_ISia_zwGINzRvyQ confluentinc/cp-kafka:7.8.0)"
for attempt in $(seq 1 120); do
    if curl -fsS --max-time 1 "$s3/minio/health/live" >/dev/null 2>&1 &&
        nc -z 127.0.0.1 "$kafka_port" >/dev/null 2>&1; then break; fi
    sleep 0.25
done
aws --endpoint-url "$s3" s3api create-bucket --bucket rtdb-proof >/dev/null

for topic in "$tag-source" "$tag-resolved"; do
    docker exec "$kafka_id" kafka-topics --bootstrap-server "127.0.0.1:$kafka_port" \
        --create --if-not-exists --topic "$topic" --partitions 1 --replication-factor 1 \
        --config message.timestamp.type=LogAppendTime >/dev/null
done

sql() {
    local port="$1"
    shift
    psql -X -w -qAt -v ON_ERROR_STOP=1 -h 127.0.0.1 -p "$port" -U xtdb -d xtdb -c "$1"
}
start_worker() {
    local worker="$1" port="$2" health="$3"
    cat > "$proof_run/worker-$worker.yaml" <<YAML
nodeId: worker-$worker
server: {host: 127.0.0.1, port: $port}
healthz: {host: 127.0.0.1, port: $health}
flightSql: null
authn: !SingleRootUser
  password: rtdb-classic-proof
remotes:
  log: !Kafka
    bootstrapServers: "127.0.0.1:$kafka_port"
log: !Kafka
  cluster: log
  topic: $tag-source
  replicaTopic: $tag-resolved
  autoCreateTopic: false
storage: !Remote
  objectStore: !S3
    bucket: rtdb-proof
    prefix: $tag
    endpoint: $s3
    pathStyleAccessEnabled: true
diskCache:
  path: $proof_run/cache-$worker
  maxSizeBytes: 67108864
compactor: {threads: 0}
garbageCollector: {enabled: false}
YAML
    "$binary" node -f "$proof_run/worker-$worker.yaml" > "$proof_run/worker-$worker.log" 2>&1 &
    worker_pid="$!"
    owned_pids+=("$worker_pid")
    for attempt in $(seq 1 200); do
        if curl -fsS --max-time 1 "http://127.0.0.1:$health/healthz/ready" >/dev/null 2>&1 &&
            [[ "$(sql "$port" 'SELECT 1' 2>/dev/null)" == "1" ]]; then return 0; fi
        if ! kill -0 "$worker_pid" 2>/dev/null; then
            cat "$proof_run/worker-$worker.log" >&2
            return 1
        fi
        sleep 0.15
    done
    return 1
}
visible() {
    sql "$1" "SELECT total FROM orders ORDER BY total"
}
history() {
    sql "$1" "SELECT _id,total,_valid_from,_valid_to,_system_from,_system_to FROM orders FOR ALL SYSTEM_TIME FOR ALL VALID_TIME" | sort
}
checkpoint_keys() {
    aws --endpoint-url "$s3" s3api list-objects-v2 --bucket rtdb-proof --prefix "$tag/" \
        --query 'Contents[].Key' --output json |
        python3 -c 'import json,sys; print("\n".join(sorted(key for key in (json.load(sys.stdin) or []) if "/blocks/" in key)))'
}

start_worker 1 "$pg1" "$h1"
one="$worker_pid"
sql "$pg1" "INSERT INTO orders (_id, total) VALUES (1, 10), (2, 20), (3, 30)"
curl -fsS --max-time 20 -X POST "http://127.0.0.1:$h1/system/finish-block?db=xtdb" >/dev/null
for attempt in $(seq 1 60); do
    checkpoint="$(checkpoint_keys)"
    if [[ -n "$checkpoint" ]]; then break; fi
    sleep 0.25
done
[[ -n "$checkpoint" ]]
sql "$pg1" "UPDATE orders SET total = 11 WHERE _id = 1"
sql "$pg1" "DELETE FROM orders WHERE _id = 2"
[[ "$(visible "$pg1")" == $'11\n30' ]]
history "$pg1" > "$proof_run/before.tsv"
start_worker 2 "$pg2" "$h2"
two="$worker_pid"
start_worker 3 "$pg3" "$h3"
three="$worker_pid"
[[ "$(visible "$pg2")" == $'11\n30' && "$(visible "$pg3")" == $'11\n30' ]]
[[ "$(checkpoint_keys)" == "$checkpoint" ]]

started="$(python3 -c 'import time; print(time.monotonic_ns())')"
kill -KILL "$one" "$two" "$three"
for worker in "$one" "$two" "$three"; do
    status=0
    wait "$worker" 2>/dev/null || status="$?"
    [[ "$status" == 137 ]]
done
[[ "$(docker inspect --format '{{.State.Running}}' "$kafka_id")" == true ]]
curl -fsS --max-time 2 "$s3/minio/health/live" >/dev/null
[[ ! -e "$proof_run/cache-4" ]]
start_worker 4 "$pg4" "$h4"
[[ "$(visible "$pg4")" == $'11\n30' ]]
history "$pg4" > "$proof_run/after.tsv"
diff -u "$proof_run/before.tsv" "$proof_run/after.tsv"
python3 -c 'import sys,time; print(f"All three classic worker processes terminated, then results verified in {(time.monotonic_ns()-int(sys.argv[1]))/1e9:.3f}s")' "$started"
[[ "$(checkpoint_keys)" == "$checkpoint" ]]
sql "$pg4" "INSERT INTO orders (_id, total) VALUES (4, 40)"
[[ "$(visible "$pg4")" == $'11\n30\n40' ]]
printf 'Recovered current rows, complete bitemporal history, and a new write from retained Kafka topics and S3\n'
