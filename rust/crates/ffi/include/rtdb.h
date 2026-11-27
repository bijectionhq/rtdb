#ifndef RTDB_V1_H
#define RTDB_V1_H
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t RtdbHandle;
typedef struct { const uint8_t *data; size_t length; } RtdbSlice;

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE
struct ArrowSchema {
  const char *format;
  const char *name;
  const char *metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema **children;
  struct ArrowSchema *dictionary;
  void (*release)(struct ArrowSchema *);
  void *private_data;
};
struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void **buffers;
  struct ArrowArray **children;
  struct ArrowArray *dictionary;
  void (*release)(struct ArrowArray *);
  void *private_data;
};
#endif

/* Calls are blocking and return 0 on success, 1 on failure.
 * Every error output must be non-null and receives an owned diagnostic handle on failure.
 * Release all returned handles, including diagnostics; handle 0 is empty.
 * Inputs remain borrowed for the call; outputs require aligned, writable, disjoint storage.
 * Text is UTF-8 with explicit byte lengths; a null pointer is allowed only with length zero.
 * All entry points contain Rust panics. Invalid foreign pointers remain caller errors outside this guarantee.
 * Retain creates an independent handle identity; close affects every alias of the underlying object.
 * Released identities are never reused and return an invalid-handle error.
 * Concurrent calls are serialized per session/statement/cursor; cancellation does not acquire their locks.
 * Do not call from a Tokio runtime thread or from a foreign Arrow release callback.
 * Exclude concurrent native environment mutation while database_open reads its configuration.
 */
uint32_t rtdb_v1_abi_version(void);
int32_t rtdb_v1_database_open(const uint8_t *yaml, size_t yaml_length, size_t memory_bytes, RtdbHandle *out, RtdbHandle *error);
/* Properties is a UTF-8 JSON object of string values, at most 4096 bytes, borrowed until return.
 * Only java.util.Arrays.useLegacyMergeSort is accepted; absent selects TimSort.
 * Case-insensitive true selects legacy sorting; other strings select TimSort without trimming.
 * Unknown keys, nonstring values, invalid JSON and exhausted startup memory fail before opening node resources.
 * Duplicate keys use the last value; the resolved selection is retained for this database lifetime.
 * This reads no JVM properties; the host supplies its captured values.
 */
int32_t rtdb_v1_database_open_with_properties(const uint8_t *yaml, size_t yaml_length, size_t memory_bytes,
                                           const uint8_t *properties, size_t properties_length, RtdbHandle *out, RtdbHandle *error);
/* The callback supplies epoch seconds and a nanosecond fraction on success (0).
 * A nonzero return or an unrepresentable instant rejects the transaction append.
 * Each in-memory or local source log samples in append order, and different logs can call concurrently.
 * The callback must be callable on native threads and must not reenter RTDB.
 * Keep the callback and context alive until database close completes, including on other aliases.
 * Output pointers are writable only for the duration of the callback.
 * Properties and configuration have the same contract as database_open_with_properties.
 */
typedef int32_t (*RtdbInstantSource)(uintptr_t context, int64_t *seconds, uint32_t *nanos);
int32_t rtdb_v1_database_open_with_log_clock(const uint8_t *yaml, size_t yaml_length, size_t memory_bytes,
                                          const uint8_t *properties, size_t properties_length,
                                          RtdbInstantSource clock, uintptr_t context, RtdbHandle *out, RtdbHandle *error);
/* Requests a block flush in each open writable database and waits for indexed progress
 * through each request. timeout_nanos is a positive overall deadline.
 */
int32_t rtdb_v1_database_flush_all(RtdbHandle handle, int64_t timeout_nanos, RtdbHandle *error);
/* Waits until each open database has indexed every message its source log held when the call
 * began. timeout_nanos is a positive overall deadline.
 */
int32_t rtdb_v1_database_sync_all(RtdbHandle handle, int64_t timeout_nanos, RtdbHandle *error);
/* Listener ports are -1 when absent. Host calls write UTF-8 without a terminator.
 * A wildcard address is reported as localhost; absent listeners write length 0.
 */
int32_t rtdb_v1_database_pgwire_port(RtdbHandle database, int32_t *out, RtdbHandle *error);
int32_t rtdb_v1_database_pgwire_client_host(RtdbHandle database, uint8_t *out, size_t capacity, size_t *length, RtdbHandle *error);
int32_t rtdb_v1_database_read_only_pgwire_port(RtdbHandle database, int32_t *out, RtdbHandle *error);
int32_t rtdb_v1_database_read_only_pgwire_client_host(RtdbHandle database, uint8_t *out, size_t capacity, size_t *length, RtdbHandle *error);
int32_t rtdb_v1_database_flight_sql_port(RtdbHandle database, int32_t *out, RtdbHandle *error);
int32_t rtdb_v1_database_healthz_port(RtdbHandle database, int32_t *out, RtdbHandle *error);
/* In-process host status. Returns an owned Transit JSON byte handle.
 * Each open database name maps to its vector of partition keys, where a key is
 * [tx_id, system_time_micros] or null if that partition has never completed a transaction.
 */
int32_t rtdb_v1_database_catalog_status(RtdbHandle database, RtdbHandle *out, RtdbHandle *error);
/* Null text pointers mean an unknown user or address. Output: 0 no match,
 * 1 trust, 2 password, 3 client credentials, 4 device authorization.
 */
int32_t rtdb_v1_database_auth_method(RtdbHandle database, const uint8_t *user, size_t user_length, const uint8_t *remote, size_t remote_length, uint32_t *out, RtdbHandle *error);
int32_t rtdb_v1_retain(RtdbHandle handle, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_release(RtdbHandle handle, RtdbHandle *error);

/* Close is idempotent while its handle remains retained.
 * Closing a database cancels admitted operations and joins all native drivers before returning.
 * Already-exported Arrow arrays remain readable and retain their allocation charges.
 * Closing a statement does not close previously returned cursors.
 * Releasing the last database handle also closes it; sessions do not keep an unclosed database running.
 */
int32_t rtdb_v1_close(RtdbHandle handle, RtdbHandle *error);

/* Fields: 0 category, 1 stable code, 2 message, 3 Transit MessagePack anomaly.
 * The returned slice borrows the diagnostic until release; do not concurrently release it while reading.
 * An absent message has a null pointer and zero length; a present empty message has a nonnull pointer and zero length.
 * Emergency diagnostic 1 has an empty Transit field when the diagnostic budget is exhausted.
 */
int32_t rtdb_v1_error_field(RtdbHandle handle, uint32_t field, RtdbSlice *out, RtdbHandle *error);

/* Authentication: 0 unauthenticated embedded caller, 1 explicitly trusted user,
 * 2 password, 3 OAuth client credentials (user is client ID).
 * Modes 0 and 1 are privileged host APIs, as Xtdb.connect/authenticateTrusted are.
 */
int32_t rtdb_v1_session_open(RtdbHandle database, const uint8_t *name, size_t name_length, uint32_t authentication,
                           const uint8_t *user, size_t user_length, const uint8_t *secret, size_t secret_length,
                           RtdbHandle *out, RtdbHandle *error);
/* Authenticate with mode 1, 2 or 3. Release the returned immutable principal with rtdb_v1_release.
 * A principal opens sessions only on its originating node. Sessions retain their own reference.
 * User ID calls return owned UTF-8 byte handles or 0 when the caller has no user ID.
 */
int32_t rtdb_v1_database_authenticate(RtdbHandle database, uint32_t mode, const uint8_t *user, size_t user_length, const uint8_t *secret, size_t secret_length, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_principal_user_id(RtdbHandle principal, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_session_open_as(RtdbHandle database, const uint8_t *name, size_t name_length, RtdbHandle principal, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_session_user_id(RtdbHandle session, RtdbHandle *out, RtdbHandle *error);

/* Read the owned verification URL byte handle before waiting. The challenge is single-use.
 * Waiting returns an immutable principal. Close the challenge to cancel a wait and release it when done.
 */
int32_t rtdb_v1_database_start_device_auth(RtdbHandle database, const uint8_t *user, size_t user_length, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_device_auth_url(RtdbHandle challenge, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_device_auth_wait(RtdbHandle challenge, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_session_set_catalog(RtdbHandle session, const uint8_t *name, size_t name_length, RtdbHandle *error);
int32_t rtdb_v1_session_set_application_name(RtdbHandle session, const uint8_t *name, size_t name_length, RtdbHandle *error);
int32_t rtdb_v1_session_await_token(RtdbHandle session, RtdbHandle *out, RtdbHandle *error);

/* Operations descend from their session's cancellation lifetime.
 * Create one per operation, retain it until its cursor closes, and release after the call/stream finishes.
 * Releasing the last operation handle cancels its work; sibling operation tokens remain usable.
 */
int32_t rtdb_v1_operation_new(RtdbHandle session, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_cancel(RtdbHandle operation_or_cursor, RtdbHandle *error);

/* Actions: 0 autocommit off, 1 on, 2 begin, 3 ADBC commit, 4 ADBC rollback,
 * 5 commit sync, 6 commit async, 7 rollback transaction.
 */
int32_t rtdb_v1_session_action(RtdbHandle session, RtdbHandle operation, uint32_t action, RtdbHandle *error);

typedef struct RtdbCommitResult {
    int64_t tx_id;
    int64_t system_time_micros;
    RtdbHandle diagnostic;
    uint32_t kind;
    uint32_t reserved;
} RtdbCommitResult;
/* Commits an explicit transaction with asynchronous set to 0 or 1.
 * kind: 0 no write transaction, 1 submitted, 2 committed, 3 aborted.
 * tx_id is set for kinds 1-3; system_time_micros for kinds 2-3.
 * An indexed abort is a successful call and may transfer an owned diagnostic handle.
 * Release any nonzero diagnostic after reading it. Call failures use error as usual.
 */
int32_t rtdb_v1_session_commit_result(RtdbHandle session, RtdbHandle operation, uint32_t asynchronous, RtdbCommitResult *out, RtdbHandle *error);

/* A null token clears the await token. Null read options leave their settings unchanged. */
int32_t rtdb_v1_session_set_await_token(RtdbHandle session, RtdbHandle operation, const uint8_t *token, size_t length, RtdbHandle *error);
typedef struct RtdbReadTime {
    RtdbSlice text;
    int64_t seconds;
    uint32_t nanos;
    uint32_t kind; /* 1: SQL time text, 2: epoch parts, 3: local date, 4: local date-time, 5: zoned date-time */
} RtdbReadTime;
int32_t rtdb_v1_session_set_read_settings(RtdbHandle session, RtdbHandle operation, const uint8_t *timezone, size_t timezone_length, const uint8_t *snapshot_token, size_t snapshot_length, const RtdbReadTime *snapshot_time, const RtdbReadTime *clock_time, RtdbHandle *error);
/* A failed begin preserves the previous transaction state. Null options use the session basis and time zone. */
int32_t rtdb_v1_session_begin_read_only(RtdbHandle session, RtdbHandle operation, const uint8_t *timezone, size_t timezone_length, const uint8_t *snapshot_token, size_t snapshot_length, const RtdbReadTime *snapshot_time, const RtdbReadTime *clock_time, RtdbHandle *error);
/* The optional metadata is a borrowed Transit MessagePack map. asynchronous is 0 or 1. */
int32_t rtdb_v1_session_begin_write_only(RtdbHandle session, RtdbHandle operation, const uint8_t *timezone, size_t timezone_length, const RtdbReadTime *system_time, const uint8_t *metadata, size_t metadata_length, uint32_t asynchronous, RtdbHandle *error);
/* Shape the current transaction before its first query or write. Null options retain existing values.
 * For read-write shaping, asynchronous is 0 (sync), 1 (async), or 2 (unchanged).
 */
int32_t rtdb_v1_session_set_transaction_read_only(RtdbHandle session, RtdbHandle operation, const uint8_t *snapshot, size_t snapshot_length, const RtdbReadTime *snapshot_time, const RtdbReadTime *clock_time, const uint8_t *await_token, size_t await_length, RtdbHandle *error);
int32_t rtdb_v1_session_set_transaction_read_write(RtdbHandle session, RtdbHandle operation, const RtdbReadTime *system_time, const uint8_t *metadata, size_t metadata_length, uint32_t asynchronous, RtdbHandle *error);
int32_t rtdb_v1_session_set_transaction_timezone(RtdbHandle session, RtdbHandle operation, const uint8_t *timezone, size_t length, RtdbHandle *error);

/* bulk=0: SQL source; bulk=1: CREATE_APPEND table name.
 * A bind replaces existing arguments, preserves ordinal positions and explicit zero-row batches.
 * Valid Arrow input structs are moved into this call once decoding begins and copied before return.
 * On earlier rejection their release callbacks remain set and the caller must release them.
 * Foreign release callbacks must be non-throwing and callable from the initiating thread.
 */
int32_t rtdb_v1_statement_new(RtdbHandle session, const uint8_t *source, size_t length, uint32_t bulk, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_statement_set_sql(RtdbHandle statement, const uint8_t *source, size_t length, RtdbHandle *error);
int32_t rtdb_v1_statement_prepare(RtdbHandle statement, RtdbHandle operation, RtdbHandle *error);
int32_t rtdb_v1_statement_bind(RtdbHandle statement, struct ArrowArray *array, struct ArrowSchema *schema, RtdbHandle *error);
/* Retained C Data bind extends the lifetime of the input Arrow buffers.
 * The producer must keep the buffers immutable until release and allow release
 * from any thread. Release can follow statement replacement and cursor closure.
 * The input structs are moved on the same terms as rtdb_v1_statement_bind.
 */
int32_t rtdb_v1_statement_bind_retained(RtdbHandle statement, struct ArrowArray *array, struct ArrowSchema *schema, RtdbHandle *error);
int32_t rtdb_v1_statement_bind_transit(RtdbHandle statement, const uint8_t *msgpack_rows, size_t length, RtdbHandle *error);

/* Arrow output structs must be zero-initialized or released.
 * Each successful export has its own release callback and may outlive every native handle.
 * Release each struct exactly once according to the Arrow C Data Interface.
 * parameters=0 returns result schema; 1 returns positional parameter schema after prepare.
 */
int32_t rtdb_v1_statement_schema(RtdbHandle statement, RtdbHandle operation, uint32_t parameters, struct ArrowSchema *out, RtdbHandle *error);
int32_t rtdb_v1_statement_query(RtdbHandle statement, RtdbHandle operation, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_statement_update(RtdbHandle statement, RtdbHandle operation, int64_t *affected, RtdbHandle *error);
int32_t rtdb_v1_cursor_schema(RtdbHandle cursor, struct ArrowSchema *out, RtdbHandle *error);
int32_t rtdb_v1_cursor_next(RtdbHandle cursor, struct ArrowArray *array, struct ArrowSchema *schema, uint32_t *has_batch, RtdbHandle *error);
/* Uses the fixed schema returned by rtdb_v1_cursor_schema for every batch. */
int32_t rtdb_v1_cursor_next_array(RtdbHandle cursor, struct ArrowArray *array, uint32_t *has_batch, RtdbHandle *error);
int32_t rtdb_v1_cursor_consumer_error(RtdbHandle cursor, RtdbHandle *error);

/* Transit row export returns an owned byte handle, or 0 at end-of-stream.
 * It consumes the same cursor as Arrow export; choose one representation per stream.
 * bytes borrows its byte handle until release; do not concurrently release it while reading.
 * session_transact takes a Transit MessagePack map of operations/options/asynchronous;
 * its result is an owned Transit JSON transaction-key byte handle.
 */
int32_t rtdb_v1_cursor_next_transit(RtdbHandle cursor, RtdbHandle *out, RtdbHandle *error);
/* The byte view is valid until the returned byte handle is released. */
int32_t rtdb_v1_cursor_next_row_bytes(RtdbHandle cursor, RtdbSlice *view, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_bytes(RtdbHandle bytes, RtdbSlice *out, RtdbHandle *error);
int32_t rtdb_v1_session_transact(RtdbHandle session, RtdbHandle operation, const uint8_t *request, size_t length, RtdbHandle *out, RtdbHandle *error);
/* Takes the same request as session_transact and returns the structured transaction outcome. */
int32_t rtdb_v1_session_transact_result(RtdbHandle session, RtdbHandle operation, const uint8_t *request, size_t length, RtdbCommitResult *out, RtdbHandle *error);
/* The plan is an EDN logical relation. Arguments are a Transit MessagePack map of named values. */
int32_t rtdb_v1_session_query_ra(RtdbHandle session, RtdbHandle operation, const uint8_t *plan, size_t plan_length, const uint8_t *arguments, size_t arguments_length, RtdbHandle *out, RtdbHandle *error);
/* Takes one Arrow row with unique named fields under the retained bind ownership contract. */
int32_t rtdb_v1_session_query_ra_arrow(RtdbHandle session, RtdbHandle operation, const uint8_t *plan, size_t plan_length, struct ArrowArray *arguments, struct ArrowSchema *schema, RtdbHandle *out, RtdbHandle *error);

/* Metadata JSON: kind 0=getInfo, 1=getTableTypes, 2=getObjects, 3=getTableSchema.
 * Optional fields: info_codes, catalog, schema, table, column, table_types.
 * getObjects depth: 0=ALL, 1=CATALOGS, 2=DB_SCHEMAS, 3=TABLES; filters use SQL LIKE.
 * getTableSchema uses exact names and defaults catalog/session and schema/public.
 */
int32_t rtdb_v1_metadata(RtdbHandle session, RtdbHandle operation, const uint8_t *request_json, size_t length, RtdbHandle *out, RtdbHandle *error);
int32_t rtdb_v1_memory_used(RtdbHandle database, size_t *out, RtdbHandle *error);

#ifdef __cplusplus
}
#endif
#endif
