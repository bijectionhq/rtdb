#!/usr/bin/env bash
# Usage: run.sh <engine:jvm|rust> <image> <repetition> <rows>
set -u
engine=$1 image=$2 rep=$3 rows=${4:-100000}
B=$(cd "$(dirname "$0")" && pwd)
PY=${PYTHON:-python3}
export DOCKER_HOST=unix://$HOME/.colima/default/docker.sock
name=bench-$engine-$rep-$$; port=$((25430 + rep)); [ $engine = rust ] && port=$((25440 + rep)); fport=$((port + 100))
docker rm -f $name >/dev/null 2>&1
if [ $engine = jvm ]; then
  docker run -d --name $name --cpus 4 --memory 4g --memory-swap 4g -p 127.0.0.1:$port:5432 -p 127.0.0.1:$fport:3000 \
    -e JAVA_TOOL_OPTIONS="-Xmx2g -XX:MaxDirectMemorySize=1536m" -v $B/jvm.yaml:/config.yaml:ro \
    $image -f /config.yaml >/dev/null
else
  docker run -d --name $name --cpus 4 --memory 4g --memory-swap 4g -p 127.0.0.1:$port:5432 -p 127.0.0.1:$fport:3000 \
    -e RTDB_MEMORY_LIMIT_BYTES=3221225472 -v $B/rust.yaml:/config.yaml:ro \
    $image node --file /config.yaml >/dev/null
fi
$PY $B/bench.py --port $port --container $name --rows $rows --output $B/result-$engine-$rep.json > $B/log-$engine-$rep.txt 2>&1
status=$?
$PY $B/bench_flight.py --port $fport --rows $rows --output $B/result-flight-$engine-$rep.json > $B/log-flight-$engine-$rep.txt 2>&1
fstatus=$?
docker logs $name > $B/server-$engine-$rep.log 2>&1
docker rm -f $name >/dev/null
echo "$engine rep $rep exit $status flight $fstatus"
