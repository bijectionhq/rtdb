(ns xtdb.api-adbc-test
  "Exercises `xtdb.api` over the in-process ADBC path — an explicit `Xtdb.Connection` obtained from
  the node — plus a guard that a plain JDBC connection stays on the pgwire path."
  (:require [clojure.string :as str]
            [clojure.test :as t :refer [deftest]]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.serde :as serde]
            [xtdb.sql :as sql]
            [xtdb.test-util :as tu :refer [*node*]]
            [xtdb.time :as time]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (com.github.benmanes.caffeine.cache Cache)
           (java.time ZoneId)
           (org.antlr.v4.runtime ParserRuleContext)
           (xtdb.api.error Incorrect)
           (xtdb.arrow Relation)
           (xtdb.query PlanCacheKey)
           (xtdb.tx TxOp$PatchDocs TxOp$PutDocs TxWriter)))

(t/use-fixtures :each tu/with-mock-clock tu/with-node)

(deftest execute-tx-and-query
  (with-open [conn (xt/open-adbc-conn *node*)]
    (t/is (= (serde/->TxKey 0 (time/->instant #inst "2020-01-01"))
             (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]]))
          "execute-tx over an Xtdb.Connection returns the tx-key")

    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs"))
          "the connection reads its own write (read-your-writes)")

    (t/is (= {:tx-id 1} (xt/submit-tx conn [[:put-docs :docs {:xt/id :bar, :n 2}]]))
          "submit-tx returns the tx-id")

    (t/is (= #{{:xt/id :foo, :n 1} {:xt/id :bar, :n 2}}
             (set (xt/q conn "SELECT _id, n FROM docs"))))))

(deftest client-op-conversions
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :a, :n 1}]
                         [:put-docs :docs {:xt/id :b, :n 2}]
                         [:put-docs :docs {:xt/id :c, :n 3}]])

    (xt/execute-tx conn [[:patch-docs :docs {:xt/id :a, :n 10}]
                         [:delete-docs :docs :b]
                         [:sql "INSERT INTO docs (_id, n) VALUES ('d', ?)" [4]]])

    (t/is (= #{{:xt/id :a, :n 10} {:xt/id :c, :n 3} {:xt/id "d", :n 4}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "patch updated :a, delete removed :b, raw sql inserted the string id 'd'")

    (xt/execute-tx conn [[:erase-docs :docs :a]])
    (t/is (= #{{:xt/id :c, :n 3} {:xt/id "d", :n 4}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "erase removed :a")))

(deftest multi-row-batches
  ;; each of these converts to a single TxOp.Sql carrying a multi-row batch
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/execute-tx conn [[:sql "INSERT INTO docs (_id, n) VALUES (?, ?)" [1 10] [2 20] [3 30]]])
    (t/is (= #{{:xt/id 1, :n 10} {:xt/id 2, :n 20} {:xt/id 3, :n 30}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "a batched :sql insert applies every arg-row")

    (xt/execute-tx conn [[:delete-docs :docs 1 2]])
    (t/is (= #{{:xt/id 3, :n 30}}
             (set (xt/q conn "SELECT _id, n FROM docs")))
          "delete-docs with several ids removes them all")))

(deftest prepared-static-insert-and-patch
  (with-open [conn (xt/open-adbc-conn *node*)]
    (let [al (.getAllocator conn)]
      (doseq [[sql rows] [["INSERT INTO docs (_id, title, tally, _valid_from, _valid_to)
                           VALUES (?, ?, ?, DATE '2019-01-01', DATE '2021-01-01')"
                          [[1 "one" (int 10)] [2 "two" (int 20)]]]
                         ["PATCH INTO docs FOR PORTION OF VALID_TIME FROM DATE '2019-06-01' TO DATE '2020-06-01'
                           RECORDS {_id: ?, title: ?}"
                          [[2 "patched"]]]]]
        (with-open [stmt (.createStatement conn sql)]
          (.prepare stmt)
          (with-open [args (apply vw/open-args al rows)]
            (.bind stmt args))
          (.executeUpdate stmt))))

    (t/is (= [{:xt/id 1, :title "one", :tally 10}
              {:xt/id 2, :title "patched", :tally 20}]
             (xt/q conn "SELECT _id, title, tally FROM docs
                         FOR VALID_TIME AS OF DATE '2020-01-01' ORDER BY _id")))
    (t/is (= [{:title "two"}]
             (xt/q conn "SELECT title FROM docs FOR VALID_TIME AS OF DATE '2019-01-01' WHERE _id = 2")))))

(defn- open-row-static-ops [al sql args tz]
  (util/safe-mapv
   (fn [{:keys [op docs valid-from valid-to]}]
     (let [vf (time/->instant valid-from {:default-tz tz})
           vt (time/->instant valid-to {:default-tz tz})]
       (case op
         :put-docs (TxOp$PutDocs/openFromRows al "public" "docs" docs vf vt)
         :patch-docs (TxOp$PatchDocs/openFromRows al "public" "docs" docs vf vt))))
   (sql/sql->static-ops sql args)))

(deftest prepared-static-dml-source-bytes
  (with-open [conn (xt/open-adbc-conn *node*)]
    (let [al (.getAllocator conn)
          tz (ZoneId/of "America/New_York")
          payload (byte-array [0 42 -1])]
      (doseq [[sql rows] [["INSERT INTO docs (_id, title, payload, score, _valid_from, _valid_to)
                           VALUES ($1, $2, $3, $4, $5, NULL), ($6, $7, $8, $9, $10, DATE '2021-01-01')"
                          [[1 "héllo" payload nil #xt/date "2020-01-01" 2 "" payload (int 20) #xt/date "2020-03-01"]
                           [3 "world" nil (short 30) #xt/date "2020-01-01" 4 "later" nil (int 40) #xt/date "2020-04-01"]]]
                         ["PATCH INTO docs FOR PORTION OF VALID_TIME FROM ? TO NULL
                           RECORDS {_id: ?, title: ?, score: ?}, {_id: ?, payload: ?}"
                          [[#xt/date "2020-01-01" 1 "héllo" -0.0 2 payload]
                           [#xt/date "2020-01-01" 3 nil nil 4 nil]
                           [#xt/date "2020-03-01" 5 "later" 10.0 6 payload]]]
                         ["INSERT INTO docs (_id, bool_val, i8, i16, i32, i64, f32, f64, title, payload, birthday, token)
                           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
                          [[1 true (byte 8) (short 16) (int 32) (long 64) (float -0.0) 0.5 "wide" payload
                            #xt/date "2020-01-01" #uuid "00000000-1234-4567-89ab-123456789abc"]]]
                         ["INSERT INTO docs (_id, value) VALUES (?, ?), (?, ?)"
                          [[1 #xt/clj-form :first 2 "second"]]]
                         ["INSERT INTO docs (_id, score) VALUES (?, ?), (?, ?), (?, ?), (?, ?)"
                          [[1 (int 1) 2 (long 2) 3 (int 3) 4 4.0]]]]]
        (util/with-open [expected (with-open [args (apply vw/open-args al rows)]
                                    (open-row-static-ops al sql args tz))
                         actual (with-open [args (apply vw/open-args al rows)]
                                  (sql/sql->tx-ops sql args al tz {}))]
          (t/is (seq actual))
          (t/is (= (TxWriter/toArrowBytes expected al)
                   (TxWriter/toArrowBytes actual al))))))))

(deftest prepared-static-dml-decodes-malformed-utf8
  (with-open [conn (xt/open-adbc-conn *node*)]
    (let [al (.getAllocator conn)
          sql "INSERT INTO docs (_id, title) VALUES (?, ?)"]
      (with-open [args (Relation. al)]
        (.writeLong (.vectorFor args "?_0" #xt.arrow/type :i64 false) 1)
        (.writeBytes (.vectorFor args "?_1" #xt.arrow/type :utf8 false) (byte-array [-61 40]))
        (.endRow args)
        (util/with-open [expected (open-row-static-ops al sql args time/utc)
                         actual (sql/sql->tx-ops sql args al time/utc {})]
          (t/is (seq actual))
          (t/is (= (TxWriter/toArrowBytes expected al)
                   (TxWriter/toArrowBytes actual al))))))))

(deftest prepared-static-insert-missing-id-diagnostic
  (with-open [conn (xt/open-adbc-conn *node*)
              stmt (.createStatement conn "INSERT INTO docs (_id, title) VALUES (?, ?)")]
    (.prepare stmt)
    (with-open [args (vw/open-args (.getAllocator conn) [nil "first"] [nil "second"])]
      (.bind stmt args))
    (let [failure (try (.executeUpdate stmt) nil (catch Incorrect e e))
          doc (:doc (ex-data failure))]
      (t/is (= "missing '_id'" (some-> failure ex-message)))
      (t/is (map? doc))
      (t/is (= {"_id" nil, "title" "first"} doc)))))

(deftest prepared-static-insert-closes-earlier-valid-time-groups-on-failure
  (with-open [conn (xt/open-adbc-conn *node*)
              al (.newChildAllocator (.getAllocator conn) "static-insert" 0 Long/MAX_VALUE)
              args (vw/open-args al [1 "ok" #xt/date "2020-01-01"]
                                   [nil "missing" #xt/date "2020-02-01"])]
    (let [before (.getAllocatedMemory al)]
      (t/is (thrown-with-msg? Incorrect #"missing '_id'"
                              (sql/sql->tx-ops "INSERT INTO docs (_id, title, _valid_from) VALUES (?, ?, ?)"
                                               args al time/utc {})))
      (t/is (= before (.getAllocatedMemory al))))))

(deftest prepared-static-insert-converts-valid-time-before-checking-id
  (with-open [conn (xt/open-adbc-conn *node*)]
    (let [al (.getAllocator conn)]
      (with-open [args (vw/open-args al [nil "missing" 1])]
        (t/is (thrown-with-msg? IllegalArgumentException #"No implementation of method: :->instant"
                                (sql/sql->tx-ops "INSERT INTO docs (_id, title, _valid_from) VALUES (?, ?, ?)"
                                                 args al time/utc {})))))))

(deftest query-args-and-plan-q
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]
                         [:put-docs :docs {:xt/id :bar, :n 2}]])

    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn ["SELECT _id, n FROM docs WHERE _id = ?" :foo]))
          "positional args bind on the in-process path")

    (t/is (= [1 2]
             (sort (into [] (map :n) (xt/plan-q conn "SELECT n FROM docs"))))
          "plan-q streams rows over the in-process path")))

(deftest submit-tx-then-query-sees-write
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/submit-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]])
    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs"))
          "an async submit-tx is awaited by a subsequent read on the same connection")))

(deftest invalid-system-time-is-an-anomaly
  (with-open [conn (xt/open-adbc-conn *node*)]
    (t/is (anomalous? [:incorrect nil]
                      (xt/submit-tx conn [[:put-docs :docs {:xt/id 1}]] {:system-time "foo"}))
          "a bad system-time surfaces as an :incorrect anomaly, not a raw parse exception")))

(deftest status-over-in-process
  (with-open [conn (xt/open-adbc-conn *node*)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo}]])
    (let [status (xt/status conn)]
      (t/is (map? status))
      (t/is (contains? status :latest-completed-txs)))))

(deftest node-routes-database-opt
  ;; the in-process node must route :database to that db, not silently use the default — an unknown
  ;; db surfaces (rather than the write landing in xtdb)
  (t/is (anomalous? [:incorrect nil]
                    (xt/execute-tx *node* [[:put-docs :docs {:xt/id 1}]] {:database :no-such-db}))
        "an unknown :database is rejected"))

(deftest connection-rejects-database-opt
  ;; a connection is already bound to its db, so selecting one must fail rather than silently write to
  ;; the bound db — the same contract the JDBC connectables enforce
  (with-open [conn (xt/open-adbc-conn *node*)]
    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (xt/execute-tx conn [[:put-docs :docs {:xt/id 1}]] {:database :some-db})))

    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (xt/submit-tx conn [[:put-docs :docs {:xt/id 1}]] {:database :some-db})))

    (t/is (anomalous? [:incorrect :cannot-set-db]
                      (into [] (xt/plan-q conn "SELECT 1" {:database :some-db}))))))

(deftest jdbc-path-still-works
  ;; a plain java.sql.Connection stays on the pgwire path — the protocol dispatch must not capture it
  (with-open [conn (jdbc/get-connection *node*)]
    (xt/execute-tx conn [[:put-docs :docs {:xt/id :foo, :n 1}]])
    (t/is (= [{:xt/id :foo, :n 1}]
             (xt/q conn "SELECT _id, n FROM docs")))))

(t/deftest concurrent-writers-awaits-share-one-plan-6216
  (xt/execute-tx *node* [[:put-docs :docs {:xt/id -1}]])

  (let [^Cache plan-cache (:plan-cache (.getQuerySource (util/node-base *node*)))
        tx-await-plan-count (fn []
                              (->> (keys (.asMap plan-cache))
                                   (filter (fn [^PlanCacheKey k]
                                             (let [ast (.getAst k)]
                                               (and (instance? ParserRuleContext ast)
                                                    (str/includes? (.getText ^ParserRuleContext ast) "xt.txs")))))
                                   count))]

    (loop [round 0]
      (->> (range 4)
           (mapv (fn [writer]
                   (future
                     (dotimes [i 25]
                       (xt/execute-tx *node* [[:put-docs :docs {:xt/id (+ (* writer 100) i)}]])))))
           (run! deref))

      (when (and (zero? (tx-await-plan-count)) (< round 20))
        (recur (inc round))))

    (t/is (= 1 (tx-await-plan-count))
          "every writer whose await falls back to xt.txs reuses the one plan")))
