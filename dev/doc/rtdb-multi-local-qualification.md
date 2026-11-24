# Local Multi qualification

Use the selected prebuilt `rtdb` with the `multi` feature and the corresponding `bijection-backend`.
The fixture uses the native Multi engine with a durable local FileJournal, filesystem object store and local generation keys.
It serves the `xtdb` attachment over password-authenticated Flight on IPv4 loopback.

## Start RTDB

Set `RTDB` and `BIJECTION_BACKEND` to the absolute paths of the two binaries.
Run the following in a disposable shell with OpenSSL, Python and `jq`.

```bash
set -e
umask 077
work=$(mktemp -d)
tls="$work/tls"
mkdir "$tls" "$work/bijection" "$work/bijection-keys"
id=$(python3 -c 'import uuid; print(uuid.uuid4())')
assignment=$(python3 -c 'import uuid; print(uuid.uuid4())')

cat >"$tls/ca.cnf" <<'EOF'
[req]
distinguished_name = dn
x509_extensions = ca
[dn]
[ca]
basicConstraints = critical,CA:TRUE
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
EOF
cat >"$tls/server.ext" <<'EOF'
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = IP:127.0.0.1
EOF
openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 2 \
  -config "$tls/ca.cnf" -subj /CN=RTDB-local-qualification-CA \
  -keyout "$tls/ca.key" -out "$tls/ca.pem"
openssl req -new -newkey rsa:2048 -nodes -subj /CN=RTDB-local-qualification \
  -keyout "$tls/key.pem" -out "$tls/server.csr"
openssl x509 -req -sha256 -days 2 -in "$tls/server.csr" -CA "$tls/ca.pem" \
  -CAkey "$tls/ca.key" -CAcreateserial -extfile "$tls/server.ext" \
  -out "$tls/certificate.pem"
printf '%s' "$(openssl rand -hex 32)" >"$tls/password"

"$RTDB" dev --multi --data "$work/rtdb" --multi-tls-dir "$tls" \
  --incarnation "$id" --flight-sql-port 0 >"$work/rtdb-ready.json" 2>"$work/rtdb.log" &
rtdb_pid=$!
```

The TLS directory contains the leaf PEM and any intermediates in `certificate.pem`, the PEM key in `key.pem`, and exactly 64 hexadecimal bytes in `password`.
On Unix the three input files must be private regular files.
The development data directory retains the chosen UUID in `database-id` and refuses a different UUID on reopen.

Wait for a complete JSON line on stdout while the RTDB process is alive.
The readiness line reports the ordered writer epoch after the catalog confirms the exact registration and the local listener's attachment has the expected UUID.
The `RTDB listeners bound` line on stderr reports socket allocation.

```bash
for attempt in $(seq 1 600); do
  kill -0 "$rtdb_pid" || { cat "$work/rtdb.log" >&2; exit 1; }
  if jq -e --arg id "$id" \
    '.database == "xtdb" and .incarnation == $id and .writer_epoch > 0 and
     (.native_flight_url | test("^https://127[.]0[.]0[.]1:[1-9][0-9]{0,4}$"))' \
    "$work/rtdb-ready.json" >/dev/null 2>&1 && [ -s "$work/rtdb-ready.json" ]; then
    break
  fi
  sleep 0.1
done
flight=$(jq -er --arg id "$id" \
  'select(.database == "xtdb" and .incarnation == $id and .writer_epoch > 0) |
   .native_flight_url | select(test("^https://127[.]0[.]0[.]1:[1-9][0-9]{0,4}$"))' \
  "$work/rtdb-ready.json") || { cat "$work/rtdb.log" >&2; exit 1; }
flight_port=${flight##*:}
python3 - "$tls/ca.pem" "$flight_port" <<'PY'
import socket
import ssl
import sys

context = ssl.create_default_context(cafile=sys.argv[1])
context.set_alpn_protocols(["h2"])
with socket.create_connection(("127.0.0.1", int(sys.argv[2])), timeout=5) as tcp:
    with context.wrap_socket(tcp, server_hostname="127.0.0.1") as tls:
        if tls.selected_alpn_protocol() != "h2":
            raise RuntimeError("Flight did not negotiate HTTP/2")
PY
```

## Start Bijection

Choose unused loopback API and site ports.
The binding's `instance` matches `--instance-name` and its `database` selects `xtdb`.
Bijection's native Original path uses `native_flight_url` for startup and requires the legacy `host` and positive `port` fields in the binding shape.
For this Flight-only fixture, those fields identify the same loopback endpoint.
Keep the optional `BIJECTION_RTDB_ORIGINAL_CUSTODY_PROVIDER` unset for the fresh clear Root startup case.

```bash
instance=multi-local-qualification
api_port=3210
site_port=3211
jq -n --arg instance "$instance" --arg id "$id" --arg assignment "$assignment" \
  --arg flight "$flight" --argjson port "$flight_port" \
  --rawfile password "$tls/password" --rawfile ca "$tls/ca.pem" \
  '{instance: $instance, incarnation: $id, placement_epoch: 1,
    placement_assignment: $assignment, host: "127.0.0.1", port: $port,
    database: "xtdb", user: "xtdb", password: $password,
    ca_certificate: $ca, native_flight_url: $flight}' >"$work/binding.json"

unset BIJECTION_PERSISTENCE_NAME BIJECTION_RTDB_ORIGINAL_CUSTODY_PROVIDER
BIJECTION_INSTANCE_SECRET=$(openssl rand -hex 32)
BIJECTION_ENGINE_CUSTODY_SECRET=$(openssl rand -hex 32)
export BIJECTION_INSTANCE_SECRET BIJECTION_ENGINE_CUSTODY_SECRET
"$BIJECTION_BACKEND" --self-hosted-egress --local-development \
  --interface 127.0.0.1 --port "$api_port" --site-proxy-port "$site_port" \
  --instance-name "$instance" --rtdb-binding "$work/binding.json" \
  --rtdb-incarnation "$id" --local-storage "$work/bijection" \
  --protected-key-directory "$work/bijection-keys"
```

From a second terminal, check that `curl --fail --silent --show-error http://127.0.0.1:3210/instance_name` returns `multi-local-qualification`.
Use the chosen API port if it differs.
The backend binds HTTP after it authenticates the native Original, validates the incarnation in the returned v1 Basis, applies or reopens the Root writer claim, and completes the held startup reads.
Stop the backend with Ctrl-C, then use `kill -TERM "$rtdb_pid"` and `wait "$rtdb_pid"` in the launch shell.

## Qualification cases

- Check that a blank Multi fixture reaches the Bijection HTTP route on first start.
  This exercises declaration acceptance, fresh Root writes, the retained writer receipt and the subsequent read of the installed Root.
- On a separate attempt give Bijection a different UUID in both the JSON binding and `--rtdb-incarnation` while keeping the correct Flight endpoint and credentials.
  Expect the remote Basis check to refuse before HTTP starts.
- Repeat with the exact UUID and an incorrect password, an untrusted CA, and a trusted certificate whose SAN omits `127.0.0.1`.
  Expect each attempt to refuse before HTTP starts.
- Reuse the Multi data directory with a different `--incarnation` and check that RTDB exits before publishing readiness.
  Preserve the original UUID, launch inputs and Bijection storage when checking the retained installation on restart.

The Bijection transport cases `startup_catalog_checks_only_after_declaration_acceptance` and `original_receipt_survives_a_lost_or_incomplete_reply` cover the declaration and receipt edges separately under `native_store_runtime --lib rtdb::flight_v1`.

## Separate OIDC Create and UserList profiles

Start two processes with separate fresh data directories and bootstrap UUIDs.
Pass a private UTF-8 YAML file of at most 128 KiB to each process.

```bash
"$RTDB" dev --multi --data "$data" --multi-tls-dir "$tls" \
  --incarnation "$bootstrap_id" --multi-authn "$profile" --flight-sql-port 0
```

The TLS directory supplies `certificate.pem` and `key.pem`.
Each process uses the private Flight CA that issued its IP-SAN certificate.
In this profile the YAML supplies the authenticator.

For exact Create, configure the issuer and audience of the dedicated fixture.

```yaml
authn: !OpenIdConnect
  issuerUrl: https://127.0.0.1:7777
  issuerCaCertificate: !Env RTDB_FIXTURE_ISSUER_CA
  clientId: rtdb
  clientSecret: !Env RTDB_FIXTURE_OIDC_SECRET
  workloadGrants:
    audience: local-create
    clientIdPrefix: managed-
  rules:
    - method: CLIENT_CREDENTIALS
```

The issuer CA variable contains PEM text.
Wait for the normal JSON line confirming `xtdb`, the bootstrap UUID and a positive writer epoch.
Choose an unused database name other than `xtdb` and a different nonzero UUID for the exact Create request and its version 1 grant.
The catalog reserves that pair on first Create.
Use `Authorization: Client <base64(client_id:client_secret)>` and check both reply fields.
If the call sets `x-xtdb-database`, use the grant's target name.
See [Managed workload grants](rtdb-managed-workload-grants.adoc) for the issuer wire contract.
Use the correct Flight CA when varying the issuer CA or issuer hostname.

For the fixed-scope bridge, configure each principal with explicit database grants and use Basic credentials against that separate process.

```yaml
authn: !UserList
  users:
    placement: !Argon2id 'PLACEMENT_HASH'
    conductor: !Argon2id 'CONDUCTOR_HASH'
  grants:
    placement:
      xtdb: READ_WRITE
    conductor:
      xtdb: READ_WRITE
  rules:
    - method: PASSWORD
```

Replace the placeholders with hashes from `rtdb hash-password` fed on stdin.
Use the bootstrap UUID as the exact bridge attachment when granting access to `xtdb`.
For a different bridge target, use its name in the grants and add a dedicated password principal with an empty database grant mapping and top-level `deploymentCreatePrincipals: [seed]`.
That principal can seed the bridge with an explicit name and UUID through the existing Create action.
The profile rejects wildcard database grants and requires a grant mapping for each UserList principal.
