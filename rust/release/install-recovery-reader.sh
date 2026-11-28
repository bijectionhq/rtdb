#!/bin/sh
set -eu
unset LD_PRELOAD

destination=${1:?recovery reader installation directory is required}
case "$destination" in
  /*) ;;
  *) exit 2 ;;
esac

binary=/usr/local/bin/rtdb
install -d -m 0755 "$destination/bin" "$destination/lib"
install -m 0555 "$binary" "$destination/bin/rtdb"
dependencies=$(ldd "$binary")
libraries=$(printf '%s\n' "$dependencies" | awk '
  $2 == "=>" && $3 == "not" {exit 1}
  $2 == "=>" && $3 ~ /^\// {print $3}
  $1 ~ /^\// {print $1}
')
for library in $libraries; do
  install -m 0555 "$library" "$destination/lib/${library##*/}"
done

cat > "$destination/rtdb" <<'EOF'
#!/bin/sh
set -eu
root=${0%/*}
for loader in "$root"/lib/ld-linux-*.so.*; do
  if [ -x "$loader" ]; then
    exec "$loader" --library-path "$root/lib" "$root/bin/rtdb" "$@"
  fi
done
exit 126
EOF
chmod 0555 "$destination/rtdb"
"$destination/rtdb" --version
