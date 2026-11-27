#!/bin/sh
# The default command reads /usr/local/lib/xtdb/config.yaml and keeps its data under /var/lib/xtdb, as upstream's image does.
# Earlier RTDB images read /etc/rtdb/config.yaml and kept data under /var/lib/rtdb; both still work.
set -eu
default_command() {
    { [ "$#" -eq 2 ] && [ "$1" = -f ] && [ "$2" = config.yaml ] && [ "$(pwd)" = /usr/local/lib/xtdb ]; } \
        || { [ "$#" -eq 3 ] && [ "$1" = node ] && [ "$2" = --file ] && [ "$3" = /etc/rtdb/config.yaml ]; }
}
if default_command "$@"; then
    if [ "$1" = -f ] && ! cmp -s /usr/local/lib/xtdb/config.yaml /usr/share/rtdb/default-config.yaml; then
        set -- -f /usr/local/lib/xtdb/config.yaml
    elif ! cmp -s /etc/rtdb/config.yaml /usr/share/rtdb/default-config.yaml; then
        if [ "$1" = -f ]; then
            echo "Reading the configuration mounted at /etc/rtdb/config.yaml; mount it at /usr/local/lib/xtdb/config.yaml to match upstream's image." >&2
            set -- -f /etc/rtdb/config.yaml
        fi
    elif [ ! -e /var/lib/xtdb/log ] && [ ! -e /var/lib/xtdb/buffers ] \
        && { [ -e /var/lib/rtdb/log ] || [ -e /var/lib/rtdb/storage ]; }; then
        echo "Serving the data an earlier RTDB image left under /var/lib/rtdb; move it to /var/lib/xtdb/log and /var/lib/xtdb/buffers to match upstream's image." >&2
        set -- -f /etc/rtdb/config-var-lib-rtdb.yaml
    elif [ "$1" = -f ]; then
        set -- -f /usr/local/lib/xtdb/config.yaml
    fi
fi
exec /usr/local/bin/rtdb "$@"
