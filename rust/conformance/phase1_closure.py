"""Records the Phase 1 closure review: area and trace conclusions, evidence hashes, and final-design-closure."""
import hashlib, json, pathlib, sys

repo = pathlib.Path(sys.argv[1])
path = repo / "rust/conformance/source-completeness.json"
ledger = json.loads(path.read_text())
PHASE2 = " Runtime comparison against the reference continues under the Phase 2 qualification findings."
AREAS = {
    "types_columnar_durable_formats": "Semantic types, logical Arrow values, durable Arrow IPC, catalogs, Transit and temporal values have production paths through rust/crates/{types,columnar,format,temporal,trie}; union legs, nulls, UUID and FixedSizeBinary round-trip through spill, IPC and wire output under test, and the bundled tzdb matches the reference JDK. The unreachable Arrow Java vector emulation was removed.",
    "storage_indexing_historical_visibility": "Local, memory and object-store storage, the live index, trie catalogs and temporal scans have production paths through rust/crates/{storage,index,query}; live catalog tables list only unflushed rows and declared column types no longer make written columns nullable, as in the reference.",
    "sql_xtql_execution": "SQL and XTQL statements parse, bind and execute through rust/crates/{sql,xtql,plan,expr,query} on every frontend, including NEST_ONE/NEST_MANY, standalone RECORDS, GROUP BY quantifiers, pg_catalog constants, regex host forms and registered Transit rendering; grammar-rejected constructs raise the reference parse error and the remaining divergences are listed in the resolved findings.",
    "writes_replication_recovery": "Submission, transaction resolution, replica append, term admission, recovery and block production run through rust/crates/{tx,replication,node}; static DML releases per-row evaluation memory and heterogeneous documents encode with null-leg unions.",
    "compaction_gc_adapters": "Compaction, GC, PostgreSQL CDC, Kafka log and Connect, S3, Azure and GCS adapters have production paths through rust/crates/{maintenance,pg-source,kafka,s3,azure,gcs}; Kafka accepts Java client properties, reads zstd, accounts every Fetch codec and drives Connect end to end, and its test-only emulation was removed.",
    "client_operational_surfaces": "Pgwire, Flight SQL, embedded ADBC/JVM bindings, configuration, TLS, metrics and the server run through rust/crates/{pgwire,flight-sql,ffi,server,config,tls,metrics} and bindings/jvm; a native node is a JDBC DataSource over its own listener, and syntax errors carry the reference text on every frontend.",
    "conformance_machinery": "The corpus runner (bindings/jvm corpus task, rust/conformance/run_corpus.sh, corpus_summary.py) executes original test Vars against the native engine, and rust/conformance/unsupported-inventory.json classifies every production unsupported branch.",
}
TRACE_NOTE = {
    "client_read": "The public read path through pgwire, Flight SQL, embedded ADBC and the node session reaches native preparation, temporal scans and owned output; its open language findings are resolved.",
}
CLOSURE = (
    "Every area and trace names concrete production paths, re-reviewed at the Phase 1 closure revision with refreshed evidence hashes. "
    "rust/conformance/unsupported-inventory.json lists every production unsupported code with its sites, and every native-only code carries a category in "
    "unsupported-classification.json (reference-failure, resource-or-format-guard, internal-guard, jvm-host-dependent, deferred-parity, harness). "
    "Test-only JVM-internal emulation was removed: the columnar Arrow Java vector cells, Kafka producer Metadata, ordered Metadata and connection observation, Registry input windows and observed cursors, "
    "Avro schema objects and the remaining compiler-proven dead code, about 60,000 lines. JVM database delegation is absent: bindings/jvm has no xtdb-core dependency, and Xtdb/Xtdb$Connection remain the documented migration boundary."
)

def sha(p): return hashlib.sha256((repo / p).read_bytes()).hexdigest()
for section, notes in (("areas", AREAS), ("traces", TRACE_NOTE)):
    for name, entry in ledger[section].items():
        missing = [p for p in entry["implementation_paths"] if not (repo / p).exists()]
        if missing: raise SystemExit(f"{name} has missing paths {missing}")
        if name in notes: entry["conclusion"] = notes[name] + PHASE2
        entry["status"] = "reviewed"
        evidence = {p: sha(p) for p in entry["implementation_paths"]}
        for extra in (entry.get("evidence") or {}):
            if extra not in evidence and (repo / extra).exists(): evidence[extra] = sha(extra)
        entry["evidence"] = evidence
for finding in ledger["findings"]:
    if finding["id"] == "final-design-closure" and finding["status"] == "open":
        finding["status"] = "resolved"; finding["resolution"] = CLOSURE
path.write_text(json.dumps(ledger, indent=2, ensure_ascii=False) + "\n")
print([(f["id"], f["kind"]) for f in ledger["findings"] if f["status"] == "open"])
