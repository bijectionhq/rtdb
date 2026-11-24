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

## Protected Original custody profile

Start a separate Multi process with the password-authenticated `xtdb` profile, its own data directory and the Bijection Original custody provider mounted.

```bash
"$RTDB" dev --multi --data "$data" --multi-tls-dir "$tls" \
  --incarnation "$id" --custody-provider-config "$providers" --flight-sql-port 0
```

Use the Original provider YAML and bearer environment projection used by the standalone launcher. The private YAML file has a 64 KiB limit and must specify one provider for database `xtdb` and the exact launch incarnation. Its optional `owner` must be `xtdb`. For a locally issued provider certificate, put the PEM CA bundle in the provider entry's `caCertificate` field. RTDB uses that bundle for provider HTTPS and verifies the provider hostname or IP SAN. The Flight TLS inputs remain separate from provider HTTPS.

For Bijection, set `BIJECTION_RTDB_ORIGINAL_CUSTODY_PROVIDER` to its private provider JSON and use the same provider endpoint, bearer, signing public key, exact authority and prepared writer claim. Use a live supervisor that can prove positive producer exit for the restart cases.

Check startup rejects a provider configured for another incarnation or owner. Exercise missing placement and missing or mismatched response evidence over both HTTP versions. Exercise an admitted response racing a family fence, then recover the exact reservation through positive response closure while the supervisor retains the original producer exit.

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

## Combined OIDC Create and assignment profile

Use a third, independent native Multi process with a fresh data directory and bootstrap UUID.
Keep the separate OIDC Create and fixed UserList runs as independent evidence.
Reuse the loopback Flight certificate and readiness procedure above with a fresh Flight TLS directory.
Keep all YAML, keys, credential responses and attempt exports in owner-only files in the disposable directory.

Use two separate loopback HTTPS OIDC origins with different issuing CAs and leaf IP SANs.
The sample addresses allow the Bijection Create fixture to keep its existing `127.0.0.3:443` OIDC listener and put assignment OIDC on `127.0.0.4:443`.
Bind only addresses available in the isolated qualification environment.
Use the OIDC server CA for each issuer's discovery, token and UserInfo requests.
The acquisition listeners' mTLS roots and the RTDB Flight CA have their own inputs.

```yaml
authn: !ManagedWorkloadOidc
  create:
    issuerUrl: https://127.0.0.3/
    issuerCaCertificate: !Env RTDB_LOCAL_CREATE_OIDC_CA
    audience: local-rtdb-create
    clientIdPrefix: local-create-
  assignment:
    issuerUrl: https://127.0.0.4/
    issuerCaCertificate: !Env RTDB_LOCAL_ASSIGNMENT_OIDC_CA
    audience: local-rtdb-assignment
    clientIdPrefix: local-assignment-
    authorityPoint:
      relation: public.app_installation
      rowId: authority
      validAtMicros: 0
```

Match each audience and prefix to its issuer and use the same serialized issuer URL as its receipt identity.
`public.app_installation` is the generic RTDB fixture point on `main`.
For an existing application Original, configure the application's exact registered authority point.

```bash
RTDB_LOCAL_CREATE_OIDC_CA=$(cat "$work/create-oidc-ca.pem") \
RTDB_LOCAL_ASSIGNMENT_OIDC_CA=$(cat "$work/assignment-oidc-ca.pem") \
  "$RTDB" dev --multi --data "$work/rtdb-combined" --multi-tls-dir "$combined_tls" \
  --incarnation "$combined_bootstrap_id" --multi-authn "$work/combined.yaml" \
  --flight-sql-port 0 >"$work/combined.ready" 2>"$work/combined.stderr" &
combined_pid=$!
```

Start both issuers before RTDB discovers them.
Verify readiness against `combined_bootstrap_id` and verify Flight TLS against `combined_tls/ca.pem` as above.
Use the endpoint from this readiness record for both workload calls.
In separate fresh startups, replace one configured OIDC CA at a time with the other CA and require startup to fail before Flight readiness.

The Bijection harness `crates/cell_management/qualification/local-rtdb.sh` and `rtdb_admin::qualification::durable_create_oidc` provide the separate real Create and UserList fixtures.
Reuse the real Create harness's enrolled cell identity and accepted durable INSTALL for the v1 acquisition.
Hold that INSTALL while exercising its current grant.
Prepare the v2 case on an exact ready Original in the combined Multi process with an empty authority point, a separately admitted installation and its live reserved first target.
Separate deployment names and UUIDs let both grants stay valid if placement preparation requires completion of the other INSTALL.
Provision the assignment target first with its own accepted INSTALL and exact Create grant, then retain its admitted installation and placement intent.
The v2 issuer must revalidate the stamped first placement intent, matching installation digest, placement epoch 1, assignment, holder and target before issuing the exact `advance-placement` grant with empty predecessor.
Acquire through the issuer's separate private mTLS listener using its enrolled placement-controller leaf for that catalog history.
Use the retained original Advance protobuf body from `PreparedSubmission.body` in the stamped attempt record and pause its automatic submission until the probe runs.
The Bijection runtime retains the exact body before the acquisition call and uses the same namespace and key for submission.
The separate Bijection harness currently has no combined first-placement fixture that prepares this evidence and pauses the submission.

After preparing both authorities, save the complete Create acquisition response as `create-credential.json` and the version 2 delivery as `assignment-credential.json`.
Preserve the v2 delivery's `issuer`, `sub`, grant ID and scope.
Write the retained protobuf body bytes as `first-advance.pb` without re-encoding it.
Write a private `combined-fixture.json` beside those files with the actual Flight socket and the Flight CA path.

```json
{
  "flight_address": "127.0.0.1:32123",
  "flight_ca": "combined-tls/ca.pem",
  "create_credential": "create-credential.json",
  "assignment_credential": "assignment-credential.json",
  "frozen_advance_body": "first-advance.pb"
}
```

Use the matching prebuilt `rtdb-flight-sql` `flight` integration test executable with Multi support.
This ignored probe uses the existing Flight fixture client and requires a private manifest.

```bash
RTDB_COMBINED_WORKLOAD_FIXTURE="$work/combined-fixture.json" "$RTDB_FLIGHT_TEST" \
  v1_contract::managed_workload_qualification::combined_local_workload_oidc_exact_roles \
  --exact --ignored
```

Require one passed probe from the executable.
The Create credential confirms the exact reserved name and UUID and receives `PermissionDenied` for the typed Advance action.
The assignment credential receives `PermissionDenied` for Create of its own exact deployment and a fresh `Then` for the frozen first Advance with the expected receipt namespace, key and database incarnation.
Both rejections follow successful `Client` handshakes and present the database of the credential being checked.
Retain the accepted INSTALL, the stamped v2 first intent and attempt, the exact source revisions and the probe result as separate evidence.

## One Multi writer and three durable keepers

Use a prebuilt Logkeeper executable with the `stream` feature and the protocol revision pinned by the RTDB source.
Use the same loopback Flight TLS setup above, including the `password` file, and a fresh RTDB data path.
The development writer keeps object storage and keys on the local filesystem.
With `--multi-journal-url`, it connects through the production StreamJournal and sends ordered transaction batches through the shared stream writer.

Each keeper needs its own nonzero UUID, loopback address, persistent directory and server certificate.
Use the same journal CA for the three server certificates and the writer's client certificate.
Set the keeper's URI SAN to `urn:logkeeper:keeper:<keeper-uuid>` and the writer's URI SAN to `urn:logkeeper:client:rtdb-local-w14`.
The server certificates need `serverAuth` and the writer certificate needs `clientAuth`.
The normal journal transport verifies the chain and keeper URI identities.
The separate Flight endpoint still uses the IP-SAN and CA verification above.

Use the following keeper JSON shape for each process.
Give the other two keepers their own UUID, paths, zone and port, such as 5449 and 5450.

```json
{
  "keeper": "0688c510-835e-4d3a-8b3e-0ff6ce533f01",
  "listen": "127.0.0.1:5448",
  "data": "/private/w14/keeper-1",
  "zone": "local-1",
  "tls": {
    "certificate": "/private/w14/keeper-1.pem",
    "private_key": "/private/w14/keeper-1-key.pem",
    "ca": "/private/w14/journal-ca.pem"
  },
  "clients": ["urn:logkeeper:client:rtdb-local-w14"],
  "writers": ["urn:logkeeper:client:rtdb-local-w14"],
  "limits": {
    "stream_quota_bytes": 8589934592,
    "max_record_bytes": 33554432,
    "round_bytes_per_stream": 4194304,
    "round_requests": 64
  }
}
```

Run `logkeeper keeper --config KEEPER.json` once per keeper.
Keep all three processes and their durable media available before RTDB starts.
Place the private writer PEM files at `journal-certificate.pem`, `journal-key.pem` and `journal-ca.pem` in the Flight TLS directory.
Here `journal-ca.pem` is the journal CA, and `ca.pem` in the earlier Flight commands is the Flight CA.

```bash
journal_url=logkeeper+tls://127.0.0.1:5448,127.0.0.1:5449,127.0.0.1:5450
"$RTDB" dev --multi --data "$work/rtdb-w14" --multi-tls-dir "$tls" \
  --incarnation "$id" --multi-journal-url "$journal_url" --flight-sql-port 0
```

The selector accepts exactly three distinct loopback socket addresses.
Reuse the same data path only with the same URL order, keeper identities and keeper media.
For a fresh trial, choose a fresh bootstrap UUID and data path.
Wait for the exact writer readiness JSON and verify Flight TLS before beginning the authorized W14 run.
Use the verified Flight URL with the `grpc+tls://` scheme expected by the benchmark driver.

```bash
python3 rust/bench/v1_transact_commits.py "$output" --clients 32 \
  --endpoint "grpc+tls://${flight#https://}" --database xtdb --user xtdb \
  --password-file "$tls/password" --ca-file "$tls/ca.pem"
```

Record the selected keeper limits and storage layout with the comparison.
The three local processes have separate durable journals and share the test host.
