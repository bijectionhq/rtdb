(ns xtdb.datasets.edgar
  "Loads a curated slice of SEC EDGAR fundamentals into XTDB with a real
   bitemporal history.

   The dataset on S3 is the curated observation transit produced by
   xtdb.datasets.edgar.mirror (one <quarter>.transit.json.gz per quarter, the
   registry filter already applied) — but reading it needs nothing from the
   mirror: it's plain transit + the standard serde handlers, pivoted into docs by
   xtdb.datasets.edgar.parse. pg.clj reuses that same read + pivot.

   Facts are projected into wide statement tables by temporality:
   `income_statement` (duration flows), `balance_sheet` (instant balances), plus
   a static `issuer` reference.

   Filing replay (see `filing-batches`): a filing (accession) is atomic — all
   its rows commit in one transaction, never split. Filings are sorted and
   partitioned by `filed`, so whole filings sharing a date coalesce into one
   transaction, submitted in ascending order. XTDB assigns system-time itself, so
   the historical dates live on the valid-time axis.

   valid-time is when a figure became known, and differs by statement: a
   duration figure becomes known when it is filed (valid-from = filed), so a
   restatement supersedes the prior figure from its own filing date and the
   vintage trail is recoverable via FOR VALID_TIME AS OF; an instant balance is
   true as of its date (valid-from = period end)."
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [xtdb.api :as xt]
            [xtdb.datasets.edgar.parse :as parse]
            [xtdb.serde :as serde])
  (:import [java.util.zip GZIPInputStream]))

(defn read-records
  "Curated observation maps from a mirrored transit.json.gz — plain transit + the
   standard serde handlers, no EDGAR-specific code. Eager: the caller reads inside
   a with-open, so no lazy seq may outlive the stream."
  [^java.io.InputStream in]
  (vec (serde/read-transit-seq in :json)))

(defn- doc->row
  "A doc → the row map for put-docs, carrying its own valid-time as :xt/valid-from
   on the document itself. Per-doc (not the put-docs-level option) because each
   row has a distinct valid-from — instant balances are as-of their own date — so
   a shared option-level valid-from would be wrong."
  [doc]
  (let [vf (:valid-from doc)
        vt (:valid-to doc)]
    (cond-> (dissoc doc :table :valid-from :valid-to)
      vf (assoc :xt/valid-from vf)
      vt (assoc :xt/valid-to vt))))

(defn filing-batches
  "Order docs into per-filing-date transaction batches. Docs are grouped by
   :accession (a filing is atomic — never split across transactions), the filing
   groups sorted and partitioned by :filed, so each batch holds whole filings
   that share a filing date. Returns an ascending seq of [^LocalDate filed docs].

   Partition-by is safe here: it runs over filing groups we've materialised and
   sorted ourselves, not the unordered source rows."
  [docs]
  (->> (vals (group-by :accession docs))      ; whole filings, never split
       (sort-by #(:filed (first %)))          ; ascending filing date
       (partition-by #(:filed (first %)))     ; same-date filings coalesce
       (map (fn [filing-groups]
              [(:filed (ffirst filing-groups)) (apply concat filing-groups)]))))

(defn- submit-filing-batch!
  "Submit one date-batch as a transaction. One put-docs per table (a put-docs
   targets a single table); rows carry their own :xt/valid-from."
  [node docs]
  (let [tx-ops (->> (group-by #(:table (meta %)) docs)
                    (map (fn [[table table-docs]]
                           (into [:put-docs {:into table}] (map doc->row table-docs)))))]
    (xt/submit-tx node (vec tx-ops))))

(defn submit-docs!
  "Submit a seq of statement/issuer docs as a bitemporal basis: one transaction
   per filing date (see `filing-batches`), in ascending order — a later
   restatement supersedes the prior figure in valid-time from its filing date,
   leaving the prior figure valid before it. Returns the last tx-key."
  [node docs]
  (reduce (fn [_ [_filed batch-docs]] (submit-filing-batch! node batch-docs))
          nil
          (filing-batches docs)))

(defn- transit-files
  "The mirrored per-quarter transit files under `dir`, chronological. Quarters
   sort lexically (2025q2 < 2025q3 < … < 2026q1), so a name sort is publish order."
  [^java.io.File dir]
  (for [^java.io.File f (sort (.listFiles dir))
        :when (str/ends-with? (.getName f) ".transit.json.gz")]
    f))

(defn dataset
  "The mirrored EDGAR dataset under `data-dir`: a {:quarters [file ...]} map, the
   per-quarter transit files in chronological (oldest-first) order."
  [data-dir]
  {:quarters (vec (transit-files (io/file data-dir)))})

(defn submit-quarter!
  "Replays one quarter's mirrored transit file into `node`: read the curated
   observations, pivot to docs, submit per-filing-date (see `submit-docs!`).
   Only this quarter is held in memory."
  [node file]
  (with-open [in (-> (io/input-stream file) GZIPInputStream.)]
    ;; read-records realises eagerly — the transit seq is backed by `in`, which
    ;; closes when this scope exits, so no lazy seq may outlive it.
    (let [docs (parse/observations->docs (read-records in))]
      (log/infof "  %s — %d docs" (.getName (io/file file)) (count docs))
      (submit-docs! node docs))))

(defn submit-edgar!
  "Replays the mirrored EDGAR dataset into `node`, one quarter at a time so only a
   single quarter is held in memory. Quarters replay oldest-first (the `dataset`
   order)."
  [node {:keys [quarters]}]
  (log/info "Loading" (count quarters) "EDGAR quarter(s)...")
  (reduce (fn [_ file]
            (when (Thread/interrupted)
              (throw (InterruptedException. "interrupted loading EDGAR quarters")))
            (submit-quarter! node file))
          nil
          quarters)
  (log/info "EDGAR load complete."))

;;; Demo queries — run via (xt/q node [<query> & params]).

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-income-as-of-valid-time
  "An income-statement figure as it had been reported at a given instant — a
   figure's valid-from is its filing date, so FOR VALID_TIME AS OF returns the
   figure in force at T, and a restatement supersedes the prior from its own
   filing date. Run before vs after a restatement's filing date to see the value
   change. A period_end can carry both a quarterly and a year-to-date duration
   (distinct statements), so period_start is selected to tell them apart."
  ;; params: [valid-time-instant, cik, period-end-date]
  "SELECT s.period_start, s.net_income_loss, s.form, s.filed
   FROM income_statement FOR VALID_TIME AS OF ? AS s
   WHERE s.cik = ? AND s.period_end = ?
   ORDER BY s.period_start")

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-income-restatement-history
  "Every vintage of an income statement for a period_end, oldest filing first — the
   restatement trail. FOR ALL VALID_TIME yields each reported figure; where a value
   changes between consecutive `filed` dates, a restatement occurred. A period_end can
   carry both a quarterly and a year-to-date duration (distinct statements), so
   period_start is selected and ordered on to keep each duration's trail together."
  ;; params: [cik, period-end-date]
  "SELECT s.period_start, s.net_income_loss, s.form, s.filed, s.accession
   FROM income_statement FOR ALL VALID_TIME AS s
   WHERE s.cik = ? AND s.period_end = ?
   ORDER BY s.period_start, s.filed")

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-balance-as-of-valid-time
  "The assets balance effective at a real-world date — the most recent as-of
   balance valid then. Instant balances carry valid-from = their period end, so
   FOR VALID_TIME AS OF walks the genuine valid-time timeline; ordering by
   period_end desc and taking the first gives the balance in force on that date."
  ;; params: [valid-date, cik]
  "SELECT b.assets, b.period_end
   FROM balance_sheet FOR VALID_TIME AS OF ? AS b
   WHERE b.cik = ? AND b.assets IS NOT NULL
   ORDER BY b.period_end DESC
   LIMIT 1")

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(def q-issuer
  "The issuer reference for a cik, as XTDB knows it now."
  ;; params: [cik]
  "SELECT i.cik, i.entity_name FROM issuer AS i WHERE i.cik = ?")

(comment
  ;; End-to-end federation demo: Postgres is the current-state sec-master; XTDB
  ;; follows it over CDC via the EdgarPgIndexer, accruing the bitemporal history
  ;; Postgres never held. Run from the root REPL — :xtdb-datasets is on its
  ;; classpath, so `!Edgar` resolves on the node. See modules/datasets/edgar/README.md
  ;; for the prose walkthrough.

  ;; 1. Supporting cast (Kafka, Postgres, Metabase, Prometheus, Grafana):
  ;;      cd modules/datasets/edgar && docker-compose up -d
  ;;    Get the dataset first — scripts/download-dataset.sh --edgar (S3, needs AWS
  ;;    creds) or produce it locally via xtdb.datasets.edgar.mirror/mirror!.

  ;; 2. Start a queryable node bound to the compose network gateway (172.30.0.1, NOT '*' —
  ;;    reachable by the containers, but not by an untrusted LAN). The `pg` remote
  ;;    is what the attached edgar db's !Postgres source consumes; the :kafka
  ;;    log-cluster is what its !Kafka log (step 4) references by name.

  (require '[xtdb.node :as xtn]
           '[xtdb.datasets.edgar.pg :as edgar-pg] 
           '[next.jdbc :as jdbc])
  
  (def node
    (xtn/start-node
     {:server {:host "172.30.0.1" :port 5432}
      :healthz {:host "172.30.0.1" :port 8081}     ; Prometheus scrapes 172.30.0.1:8081
      ;; a whole filing coalesces into one transaction, which can exceed Kafka's 1 MB
      ;; default — bump the producer/consumer record caps (broker cap is in docker-compose.yml).
      :log-clusters {:kafka [:kafka {:bootstrap-servers "localhost:9092"
                                     :properties-map {"max.request.size" "16777216"
                                                      "max.partition.fetch.bytes" "16777216"}}]}
      :log [:kafka {:cluster :kafka :topic "xtdb.log"}]
      :storage [:local {:path "/tmp/edgar-demo/xtdb"}]
      :remotes {:pg [:postgres {:host "localhost" :database "edgar"
                                :username "postgres" :password "postgres"}]}}))

  ;; 3. ATTACH the edgar database — XTDB snapshots then tails Postgres CDC through
  ;;    the !Edgar indexer, deriving _id and valid-time per table (see EdgarPgIndexer).
  ;;    ATTACH must run against the primary `xtdb` database.
  (with-open [conn (.build (.createConnectionBuilder node))]
    (jdbc/execute! conn ["ATTACH DATABASE edgar WITH $$
  log: !Kafka { cluster: 'kafka', topic: 'xtdb.edgar' }
  storage: !Local { path: '/tmp/edgar-demo/edgar' }
  externalSource: !Postgres
    remote: 'pg'
    slotName: 'edgar_slot'
    publicationName: 'edgar_pub'
    indexer: !Edgar {}
$$"]))

  ;; 4. Load EDGAR into Postgres (psql checks + progress logging live in the
  ;;    xtdb.datasets.edgar.pg rich comment):

  (with-open [pg-conn (jdbc/get-connection 
                        {:dbtype "postgresql" :dbname "edgar"
                         :host "localhost" :port 5432
                         :user "postgres" :password "postgres"})]
    (edgar-pg/submit-edgar-pg! pg-conn (dataset "src/test/resources/data/edgar")))


  ;; 5. Query the bitemporal history against the `edgar` database. q-income-restatement-history
  ;;    et al. (above) show truth-as-of-then vs corrected-truth-now. The trail keys on
  ;;    (cik, period_end); a period_end can carry both a quarterly and a year-to-date duration,
  ;;    so period_start rides along in the output (ordered on) to keep the two apart. Find a real
  ;;    restatement (net income that actually changed across vintages) first:
  ;;      (jdbc/execute! pg-conn
  ;;        ["SELECT cik, period_start, period_end
  ;;          FROM income_statement FOR ALL VALID_TIME
  ;;          GROUP BY cik, period_start, period_end
  ;;          HAVING COUNT(DISTINCT net_income_loss) > 1 LIMIT 5"]).
  (with-open [conn (.build (-> (.createConnectionBuilder node) (.database "edgar")))]
    ;; ENERGY FOCUS, INC. FY2024 net income: +1.6M → restated -1.6M → refined -1.582M.
    (xt/q conn [q-income-restatement-history "0000924168" #xt/date "2024-12-31"]))

  (.close node))
