#!/bin/sh
# The default configuration keeps its data under /var/lib/xtdb, as upstream's image does.
# A volume an earlier RTDB image filled under /var/lib/rtdb is still served from there.
set -eu
if [ "$#" -eq 3 ] && [ "$1" = node ] && [ "$2" = --file ] && [ "$3" = /etc/rtdb/config.yaml ] \
    && [ ! -e /var/lib/xtdb/log ] && [ ! -e /var/lib/xtdb/buffers ] \
    && { [ -e /var/lib/rtdb/log ] || [ -e /var/lib/rtdb/storage ]; }; then
    echo "Serving the data an earlier RTDB image left under /var/lib/rtdb; move it to /var/lib/xtdb/log and /var/lib/xtdb/buffers to match upstream's image." >&2
    set -- node --file /etc/rtdb/config-var-lib-rtdb.yaml
fi
exec /usr/local/bin/rtdb "$@"
