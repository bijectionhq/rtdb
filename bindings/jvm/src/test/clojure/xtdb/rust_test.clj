(ns xtdb.rust-test
  (:require [clojure.test :as t :refer [deftest is]]
            [next.jdbc :as jdbc]
            [next.jdbc.result-set :as jdbc-rs]
            [xtdb.api :as xt]
            [xtdb.flight-sql :as fsql]
            [xtdb.as-of-test :as original-as-of]
            [xtdb.information-schema-test :as original-info-schema]
            [xtdb.issue-test :as original-issue]
            [xtdb.operator.apply-test :as original-apply]
            [xtdb.operator.group-by-test :as original-group-by]
            [xtdb.operator.distinct-test :as original-distinct]
            [xtdb.operator.join-test :as original-join]
            [xtdb.operator.let-test :as original-let]
            [xtdb.operator.list :as list-operator]
            [xtdb.operator.list-test :as original-list]
            [xtdb.operator.order-by-test :as original-order-by]
            [xtdb.operator.project-test :as original-project]
            [xtdb.operator.rename-test :as original-rename]
            [xtdb.operator.select-test :as original-select]
            [xtdb.operator.set-test :as original-set]
            [xtdb.operator.table-test :as original-table]
            [xtdb.operator.top-test :as original-top]
            [xtdb.sql.expr-test :as original-expr]
            [xtdb.sql.correlated-subquery-nulls-test :as original-correlated]
            [xtdb.sql.exists-decorrelation-test :as original-exists]
            [xtdb.sql.generate-series-test :as original-series]
            [xtdb.sql.interval-test :as original-interval]
            [xtdb.sql.operator-precedence-test :as original-precedence]
            [xtdb.sql.temporal-test :as original-temporal]
            [xtdb.sql.identifier-test :as original-identifiers]
            [xtdb.sql.with-test :as original-with]
            [xtdb.expression.pg-test :as original-pg-expression]
            [xtdb.serde.types :as st]
            [xtdb.node-test :as original-node]
            [xtdb.update-test :as original-update]
            [xtdb.sql-test :as original-sql]
            [xtdb.update-test :as original-update]
            [xtdb.operator.patch-test :as original-patch]
            [xtdb.operator.group-by.percentile-test :as original-percentile]
            [xtdb.operator.scan-test :as original-scan]
            [xtdb.operator.unnest-test :as original-unnest]
            [xtdb.operator.window-test :as original-window]
            [xtdb.expression.pg-test :as original-pg-expression]
            [xtdb.test-util :as tu]
            [xtdb.protocols :as protocols]
            [xtdb.rust :as rust])
  (:import [java.time InstantSource]
           [org.apache.arrow.memory RootAllocator]
           [xtdb.rust NativeDatabase]))

(def ^:private node-config "server:\n  port: 0\nflightSql: null\nnodeId: clojure-binding-test\n")

(defn- open-node ^NativeDatabase
  ([allocator] (NativeDatabase/open allocator node-config (* 512 1024 1024)))
  ([allocator ^InstantSource clock]
   (NativeDatabase/open allocator node-config (* 512 1024 1024) clock)))

(deftest api-adbc-test-query-and-native-transactions
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node [[:put-docs :docs {:xt/id :id-1, :a-b 42, :nested {:omit nil, :keep :value}}]])
    (is (= [{:xt/id :id-1, :a-b 42, :nested {:keep :value}}]
           (xt/q node "SELECT * FROM docs")))
    (is (= [{"a_b" 42}]
           (xt/q node "SELECT a_b FROM docs" {:key-fn :snake-case-string})))
    (is (= 1 (reduce (fn [n _] (reduced (inc n))) 0 (xt/plan-q node "SELECT * FROM docs"))))))

(deftest api-adbc-test-native-connection-await-and-database-rejection
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)
              connection (.connect node)]
    (is (integer? (:tx-id (xt/submit-tx connection [[:put-docs :docs {:xt/id 1}]]))))
    (is (= [{:xt/id 1}] (xt/q connection "SELECT _id FROM docs")))
    (is (thrown? xtdb.api.error.Incorrect (xt/q connection "SELECT 1" {:database :other})))))

(deftest api-adbc-test-node-rejects-an-unknown-database-as-incorrect
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (is (anomalous? [:incorrect nil "Unknown database: no_such_db"]
                    (xt/execute-tx node [[:put-docs :docs {:xt/id 1}]] {:database :no-such-db})))
    (is (anomalous? [:incorrect nil "Unknown database: no_such_db"]
                    (xt/q node "SELECT 1" {:database :no-such-db})))))

(deftest api-adbc-test-preserves-nil-vs-present-empty-sql-batch
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node ["INSERT INTO docs (_id) VALUES (1)"])
    (xt/execute-tx node [["INSERT INTO docs (_id) VALUES (?)" 2]])
    (protocols/-execute-tx node [{:op :sql, :sql "INSERT INTO docs (_id) VALUES (3)", :arg-rows []}] {})
    (is (= [{:xt/id 1} {:xt/id 2}] (xt/q node "SELECT _id FROM docs ORDER BY _id")))))

(deftest invalid-values-projection-aborts-the-logged-transaction
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node ["INSERT INTO docs (_id, v) VALUES (0, 'prior')"])
    (doseq [sql ["INSERT INTO docs (_id, v) VALUES (1)"
                 "INSERT INTO docs (_id, v) VALUES (unbound_name)"
                 "INSERT INTO docs (_id, v) VALUES (2, 'uncommitted'), (3)"]]
      (let [{:keys [tx-id]} (xt/submit-tx node [sql])
            [{:keys [committed error]}] (xt/q node ["SELECT committed, error FROM xt.txs WHERE _id = ?" tx-id])]
        (is (false? committed))
        (is (= "Errors planning SQL statement:\n  - Column count mismatch: expected 2, given 1"
               (ex-message error)))
        (is (= tx-id (-> error ex-data :tx-key :tx-id)))))
    (is (= [{:xt/id 0 :v "prior"}] (xt/q node "SELECT * FROM docs")))))

(deftest native-node-is-a-jdbc-data-source-over-its-own-pgwire-listener
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node [[:put-docs :docs {:xt/id 1 :v "native"}]])
    (with-open [conn (jdbc/get-connection node)]
      (is (= [{:_id 1 :v "native"}] (jdbc/execute! conn ["SELECT _id, v FROM docs"] {:builder-fn jdbc-rs/as-unqualified-maps}))))))

(deftest native-memory-returns-after-transactions-and-early-reduced-queries
  (doseq [client [:adbc :pgwire :flight-sql]]
    (with-open [allocator (RootAllocator.)
                node (NativeDatabase/open allocator "server:\n  port: 0\nflightSql:\n  port: 0\n" (* 512 1024 1024))]
      (let [conn (case client
                   :adbc node
                   :pgwire (xt/client {:port (.getServerPort node)})
                   :flight-sql (fsql/open-conn {:port (.getFlightSqlPort node)}))
            used #(.nativeMemoryUsed node)
            step (fn [_label f] (f))]
        (doseq [batch (partition-all 1000 (map #(hash-map :xt/id % :num %) (range 2000)))]
          (step "tx" #(xt/execute-tx conn [(into [:put-docs :docs] batch)])))
        (step "limit" #(reduce (fn [n _] (inc n)) 0 (xt/plan-q conn "SELECT docs.num FROM docs LIMIT 10")))
        (step "reduced" #(reduce (fn [n _] (let [n (inc n)] (if (== 10 n) (reduced n) n))) 0 (xt/plan-q conn "SELECT docs.num FROM docs")))
        (is (< (used) (* 16 1024 1024)) (str client))))))

(deftest transit-java-dates-work-as-documents-and-sql-arguments
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (let [date #inst "1969-12-31T23:59:59.999Z"]
      (xt/execute-tx node [[:put-docs :docs {:xt/id 1, :v date}]
                           [:sql "INSERT INTO docs RECORDS {_id: 2, v: ?}" [date]]])
      (is (= [{:xt/id 1, :v #xt/zdt "1969-12-31T23:59:59.999Z[UTC]"}
              {:xt/id 2, :v #xt/zdt "1969-12-31T23:59:59.999Z[UTC]"}]
             (xt/q node ["SELECT _id, v FROM docs WHERE v = ? ORDER BY _id" date]))))))

(defn- with-native-node [f]
  (let [[log-type {:keys [instant-src]}] (:log tu/*node-opts*)]
    (with-open [node (case log-type
                      nil (open-node tu/*allocator*)
                      :in-memory (open-node tu/*allocator* instant-src)
                      (throw (ex-info "Unsupported native log fixture" {:log-type log-type})))]
      (binding [tu/*node* node]
        (f)))))

(defn- import-logical-inputs [query]
  (let [[operator options & operands] query]
    (case operator
      ::tu/pages
      (case (count operands)
        0 [:literal-pages {:pages options}]
        1 [:literal-pages {:pages (first operands) :types (update-vals options st/render-type)}]
        (throw (ex-info "Invalid fixture page source" {:query query})))

      :list [:list (update options :batch-size #(or % list-operator/*batch-size*))]

      (into [operator options] (map import-logical-inputs) operands))))

(defn- run-native-logical-query [query {:keys [node] :as options}]
  (let [query (import-logical-inputs query)]
    (if node
      (rust/query-ra node query options)
      (with-open [node (open-node tu/*allocator*)]
        (rust/query-ra node query options)))))

(defn- run-original-api-case [original]
  (let [fixtures (-> original meta :ns meta ::t/each-fixtures)
        native-fixtures (mapv {tu/with-node with-native-node
                              tu/with-allocator tu/with-allocator
                              tu/with-mock-clock tu/with-mock-clock}
                             fixtures)
        native-fixtures (cond->> native-fixtures
                          (not-any? #{tu/with-allocator} fixtures) (into [tu/with-allocator]))]
    (when (some nil? native-fixtures)
      (throw (ex-info "Unsupported original API fixture" {:test (str original)})))
    (binding [tu/*query-ra-executor* run-native-logical-query
              tu/*flush-block-executor* (fn [^NativeDatabase node timeout] (.flushAll node timeout))]
      ((t/join-fixtures native-fixtures) (:test (meta original))))))

(defmacro ^:private deftest-original-api [original & [local-name]]
  (let [test-name (or local-name (symbol (name original)))
        test-tags (select-keys (meta (resolve original)) [:integration])]
    `(deftest ~(vary-meta test-name merge test-tags)
       (run-original-api-case (var ~original)))))

(deftest-original-api original-sql/multiple-query-tails)

(deftest-original-api original-select/test-no-column-relation)

(deftest-original-api original-select/test-select)

(deftest-original-api original-project/test-project)

(deftest-original-api original-project/test-project-row-number)

(deftest-original-api original-project/test-project-star)

(deftest-original-api original-rename/test-rename)

(deftest-original-api original-distinct/test-distinct)

(deftest-original-api original-order-by/test-order-by)

(deftest-original-api original-order-by/test-order-by-with-nulls)

(deftest-original-api original-top/test-top)

(deftest-original-api original-top/test-param-3699)

(deftest-original-api original-set/test-union-all)

(deftest-original-api original-set/test-union-all-empty-left-side-bug)

(deftest-original-api original-set/test-union-all-empty-batches-both-sides)

(deftest-original-api original-set/test-intersection)

(deftest-original-api original-set/test-difference)

(deftest-original-api original-group-by/test-group-by)

(deftest-original-api original-group-by/test-row-count)

(deftest-original-api original-group-by/test-count-empty-null-behaviour)

(deftest-original-api original-group-by/test-sum-empty-null-behaviour)

(deftest-original-api original-group-by/test-avg-empty-null-behaviour)

(deftest-original-api original-group-by/test-aggregates-over-nothing-typed-column-return-nil-6133)

(deftest-original-api original-group-by/test-min-max-over-nothing-typed-column-return-nil-6133)

(deftest-original-api original-group-by/test-min-max-over-union-of-nothing-typed-columns-return-nil-6133)

(deftest-original-api original-group-by/test-min-of-empty-rel-returns-nil)

(deftest-original-api original-group-by/test-array-agg-of-empty-rel-returns-empty-array-3819)

(deftest-original-api original-group-by/test-array-agg-orders-each-group-by-its-sort-keys)

(deftest-original-api original-group-by/test-vec-agg)

(deftest-original-api original-group-by/test-bool-aggs)

(deftest-original-api original-group-by/test-distinct group-by-distinct)

(deftest-original-api original-group-by/test-group-by-with-nils-coerce-to-boolean-npe-regress)

(deftest-original-api original-group-by/test-group-by-groups-nils)

(deftest-original-api original-group-by/test-throws-for-incomparable-minmax-340)

(deftest-original-api original-group-by/test-min-max-strings)

(deftest-original-api original-join/test-cross-join)

(deftest-original-api original-join/test-equi-join)

(deftest-original-api original-join/test-equi-join-multi-col)

(deftest-original-api original-join/test-theta-inner-join)

(deftest-original-api original-join/test-semi-equi-join)

(deftest-original-api original-join/test-semi-equi-join-multi-col)

(deftest-original-api original-join/test-theta-semi-join)

(deftest-original-api original-join/test-left-equi-join)

(deftest-original-api original-join/test-left-equi-join-multi-col)

(deftest-original-api original-join/test-left-theta-join)

(deftest-original-api original-join/test-full-outer-join)

(deftest-original-api original-join/test-full-outer-join-empty-rhs-schema-5669)

(deftest-original-api original-join/test-full-outer-equi-join-multi-col)

(deftest-original-api original-join/test-full-outer-join-theta)

(deftest-original-api original-join/test-anti-equi-join)

(deftest-original-api original-join/test-anti-equi-join-multi-col)

(deftest-original-api original-join/test-anti-join-theta)

(deftest-original-api original-join/test-join-on-true)

(deftest-original-api original-join/test-loj-on-true)

(deftest-original-api original-join/test-foj-on-true)

(deftest-original-api original-join/test-semi-join-on-true)

(deftest-original-api original-join/test-anti-join-on-true)

(deftest-original-api original-join/test-equi-join-expr)

(deftest-original-api original-join/test-single-join)

(deftest-original-api original-join/test-mark-equi-join)

(deftest-original-api original-join/test-mark-join-nils)

(deftest-original-api original-join/test-mark-join-theta)

(deftest-original-api original-join/test-mark-join-conditions-are-two-valued)

(deftest-original-api original-join/test-exists-as-mark-join)

(deftest-original-api original-apply/test-apply-operator)

(deftest-original-api original-apply/test-apply-mark)

(deftest-original-api original-apply/test-apply-single)

(deftest-original-api original-apply/test-apply-empty-rel-bug-237)

(deftest-original-api original-apply/test-nested-apply)

(deftest-original-api original-apply/test-shadowed-param)

(deftest-original-api original-apply/test-forwarding-nullable-type-information-494)

(deftest-original-api original-apply/test-missing-column-in-independent-rel)

(deftest-original-api original-let/test-let)

(deftest-original-api original-window/test-window-operator)

(deftest-original-api original-window/test-lead-lag-window-functions)

(deftest named-logical-windows-use-their-own-ordering
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (is (= #{{:a 1 :up 1 :down 3} {:a 2 :up 2 :down 2} {:a 3 :up 3 :down 1}}
           (set (rust/query-ra node
                               '[:window {:windows {ascending {:order-specs [[a]]}
                                                   descending {:order-specs [[a {:direction :desc}]]}}
                                          :projections [{up {:window-name ascending :window-agg (row-number)}}
                                                        {down {:window-name descending :window-agg (row-number)}}]}
                                 [:table {:rows [{:a 2} {:a 3} {:a 1}]}]]
                               {}))))))

(deftest-original-api original-join/can-join-on-lists-491)

(deftest-original-api original-join/can-join-on-structs-491)

(deftest-original-api original-join/full-outer-join-with-polymorphic-types)

(deftest-original-api original-join/test-left-side-built-left-outer-join)

(deftest-original-api original-set/first-tuple-in-rhs-is-taken-into-account-test)

(deftest-original-api original-table/test-table-handles-exprs)

(deftest-original-api original-table/test-table)

(deftest-original-api original-table/test-table-handles-symbols)

(deftest-original-api original-table/test-incorrect-relation-params)

(deftest-original-api original-table/do-not-leak-memory-on-expression-errors)

(deftest-original-api original-table/test-absent-columns)

(deftest-original-api original-table/test-single-col)

(deftest-original-api original-table/test-table-with-map-params)

(deftest-original-api original-sql/disallow-slashes-in-delimited-identifiers-3799)

(deftest-original-api original-sql/test-char-type)

(deftest-original-api original-sql/disallow-inserting-to-system-time-cols-3748)

(deftest-original-api original-sql/forbid-invalid-columns-insert)

(deftest static-and-dynamic-document-writes-report-their-validation-stage
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (let [payload (fn [write]
                    (try (write)
                         (catch xtdb.api.error.Incorrect error (ex-data error))))
          missing (payload #(xt/submit-tx node [[:sql "INSERT INTO docs RECORDS {v: 'value'}"]]))]
      (is (= :missing-id (:xtdb.error/code missing)))
      (is (= {"v" "value"} (:doc missing)))
      (is (= 0 (:tx-id (xt/execute-tx node [[:sql "INSERT INTO docs RECORDS {_id: 1}"]]))))
      (let [forbidden (payload #(xt/execute-tx node [[:sql "INSERT INTO docs RECORDS {_id: 2, _z: 3, _a: 4}"]]))
            dynamic (payload #(xt/execute-tx node [[:sql "INSERT INTO docs (_id, _z, _a) SELECT 3, 4, 5"]]))]
        (is (= :xtdb/forbidden-columns (:xtdb.error/code forbidden)))
        (is (= ["_z" "_a"] (:forbidden-cols forbidden)))
        (is (= :xtdb/sql-error (:xtdb.error/code dynamic)))
        (is (= 2 (get-in dynamic [:tx-key :tx-id]))))
      (is (= [{:xt/id 0, :committed true}
              {:xt/id 1, :committed false}
              {:xt/id 2, :committed false}]
             (xt/q node "SELECT _id, committed FROM xt.txs ORDER BY _id")))
      (is (= [{:xt/id 1}] (xt/q node "SELECT * FROM docs"))))))

(deftest-original-api original-pg-expression/test-oid)

(deftest-original-api original-pg-expression/test-regproc)

(deftest-original-api original-pg-expression/test-regclass)

(deftest-original-api original-pg-expression/test-regclass-where-expr)

(deftest-original-api original-pg-expression/test-regclass-search-path-precedence)

(deftest-original-api original-node/test-array-element-reference-is-one-based-336)

(deftest-original-api original-node/test-explain-plan-shows-a-mark-joins-mark)

(deftest-original-api original-node/test-explain-plan-sql)

(deftest-original-api original-node/test-explain-plan-window-fn-5990)

(deftest-original-api original-node/test-explain-plan-unnest-5990)

(deftest independent-quantified-subqueries-retain-unknown-and-empty-set-semantics
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (is (= [{:n 1, :matched true, :outside true}
            {:n 2, :outside true}
            {:outside true}]
           (xt/q node "SELECT n,
                             n IN (SELECT v FROM (VALUES (CAST(1 AS DOUBLE PRECISION)), (NULL)) c(v)) AS matched,
                             n NOT IN (SELECT 1 WHERE FALSE) AS outside
                       FROM (VALUES (1), (2), (NULL)) x(n)
                       ORDER BY n NULLS LAST")))))

(deftest-original-api original-node/test-nulling-valid-time-columns-2504)

(deftest-original-api original-node/test-indexer-cleans-up-aborted-transactions-2489)

(deftest-original-api original-with/disallow-period-specs-on-ctes-3440)

(deftest-original-api original-expr/duration-literal-query)

(deftest-original-api original-sql/plan-agg-subqueries-before-group-by-3821)

(deftest-original-api original-sql/test-select-without-from)

(deftest-original-api original-sql/test-generated-column-names)

(deftest-original-api original-identifiers/keyword-led-constructs-still-open-their-own-constructs)
(deftest-original-api original-identifiers/keyword-column-names-round-trip-through-dml)
(deftest-original-api original-identifiers/as-default-names-the-column-default)

(deftest-original-api original-sql/mismatched-columns-in-table-projection-stops-ingestion-4069)

(deftest-original-api original-sql/test-empty-array-3818)

(deftest-original-api original-sql/test-function-as-from-source)

(deftest-original-api original-sql/test-range-3212)

(deftest-original-api original-sql/test-generate-series-3212)

(deftest-original-api original-sql/test-order-nulls-first-last)

(deftest-original-api original-sql/test-xt-version)

(deftest-original-api original-expr/test-bare-user-keyword)

(deftest-original-api original-expr/test-like-regex-query-case-insensitive)

(deftest-original-api original-expr/test-ilike-query)

(deftest-original-api original-expr/test-postgres-regex-query)

(deftest-original-api original-expr/test-reverse-query)

(deftest-original-api original-expr/test-concat-function-query)

(deftest-original-api original-expr/test-position-with-substring)

(deftest-original-api original-expr/test-substring-for-length-on-nested-substring)

(deftest-original-api original-expr/test-length-query)

(deftest-original-api original-expr/test-replace)

(deftest-original-api original-expr/interval-literal-query)

(deftest-original-api original-expr/test-postgres-session-information-functions)

(deftest-original-api original-expr/test-string-to-array)

(deftest-original-api original-expr/test-array-upper)

(deftest-original-api original-expr/test-array-lower)

(deftest-original-api original-expr/test-array-length)

(deftest-original-api original-expr/test-uri-fns)

(deftest-original-api original-expr/period-nulls-4315)

(deftest-original-api original-sql/test-lateral-with-unnest-4009)

(deftest-original-api original-sql/interval-read-write-bug-4066)

(deftest-original-api original-sql/test-complex-empty-array)
(deftest-original-api original-sql/array-agg-order-by)
(deftest-original-api original-sql/test-select-star-lateral-join)
(deftest-original-api original-sql/test-select-star-subquery)
(deftest-original-api original-sql/order-by-ignored-4193)
(deftest-original-api original-sql/anonymous-params-bound-in-lexical-order-5988)
(deftest-original-api original-sql/test-all-quantified-comparison)
(deftest-original-api original-sql/test-any-correlated-array-column-4895)
(deftest-original-api original-sql/test-any-correlated-array-with-nulls-4895)
(deftest-original-api original-sql/test-is-distinct-from)
(deftest-original-api original-sql/test-in-subquery)
(deftest-original-api original-sql/datascript-test-aggregates)
(deftest-original-api original-sql/test-scalar-subquery-with-expressions)
(deftest-original-api original-sql/test-aggregate-filter)
(deftest-original-api original-sql/test-lead-lag-window-functions)
(deftest-original-api original-sql/test-operators-with-unresolved-column-references-3640)
(deftest-original-api original-sql/test-order-by-3065)
(deftest-original-api original-sql/test-order-by-unqualified-derived-column-refs)
(deftest-original-api original-sql/test-select-star-projections)
(deftest-original-api original-sql/able-to-select-star-from-two-tables-bug-3389)
(deftest-original-api original-sql/test-select-star-asterisk-clause)
(deftest-original-api original-sql/test-select-star-qualified-join)
(deftest-original-api original-sql/test-expand-asterisk-parenthesized-joined-table)
(deftest-original-api original-sql/test-invalid-xt-id-in-query-3324)
(deftest-original-api original-sql/test-case-sensitivity-in-delimited-cols)
(deftest-original-api original-sql/test-ordering-of-intersect-and-union)
(deftest-original-api original-sql/test-set-operations-with-different-column-names)
(deftest-original-api original-sql/test-union-all)
(deftest-original-api original-sql/test-union-except)
(deftest-original-api original-sql/test-join-cond-subqueries)
(deftest-original-api original-sql/test-erase-with-subquery)
(deftest-original-api original-sql/select-star-on-non-existent-tables-3005)
(deftest-original-api original-sql/nested-parenthesized-joined-tables-3185)
(deftest-original-api original-sql/test-select-same-col-twice-3025)
(deftest-original-api original-sql/test-assert-3445)
(deftest-original-api original-sql/test-assert-exists-3686-3689)
(deftest-original-api original-sql/test-portion-of-valid-time-boundary)
(deftest-original-api original-sql/contains-precedence-bug-3473)
(deftest-original-api original-sql/bound-period-contains-timestamp-expression-4381)
(deftest-original-api original-sql/unescapes-escaped-quotes-3467)
(deftest-original-api original-sql/info-schema-case-insensitivity-3511)
(deftest-original-api original-sql/show-canned-responses)
(deftest-original-api original-sql/valid-time-as-col-name-3661)
(deftest-original-api original-sql/missing-values-in-insert-shouldnt-stop-ingestion-3721)
(deftest-original-api original-sql/ignore-returning-keys-3668)
(deftest-original-api original-sql/read-write-promotion-conflict-in-live-rel-3709)

(deftest-original-api original-sql/test-no-column-name-list-insert-with-values-3752)
(deftest-original-api original-sql/test-date-id-caught-3446)
(deftest-original-api original-sql/test-tx-ops-sql-params)

(deftest sql-argument-validation-preserves-transaction-outcomes
  (tu/with-allocator
   #(tu/with-mock-clock
     (fn []
       (with-native-node
         (fn []
           (let [sql "INSERT INTO docs (_id, v) VALUES (?, ?)"
                 errors (mapv (fn [op]
                                (try (xt/execute-tx tu/*node* [op])
                                     (catch xtdb.api.error.Incorrect error (ex-data error))))
                              [[:sql sql] [:sql sql [2]]])]
             (is (= [:xtdb.indexer/missing-sql-args :xtdb.indexer/incorrect-sql-arg-count]
                    (mapv :xtdb.error/code errors)))
             (is (= [0 1] (mapv (fn [error] (get-in error [:tx-key :tx-id])) errors)))
             (is (= 2 (:tx-id (xt/execute-tx tu/*node* [[:sql sql [3 4 5]]]))))
             (is (= [{:xt/id 0, :committed false} {:xt/id 1, :committed false} {:xt/id 2, :committed true}]
                    (xt/q tu/*node* "SELECT _id, committed FROM xt.txs ORDER BY _id")))
             (is (= [{:xt/id 3, :v 4}] (xt/q tu/*node* "SELECT * FROM docs"))))))))))

(deftest-original-api original-sql/test-current-catalog)
(deftest-original-api original-sql/test-c-style-escapes-3415)
(deftest-original-api original-sql/test-delete-with-unqualified-col-3017)
(deftest-original-api original-sql/test-disallow-col-refs-in-period-specs-3447)
(deftest-original-api original-sql/forbid-updating-core-cols-3433)

(deftest-original-api original-sql/insert-with-bad-select-shouldnt-stop-ingestion-3797)
(deftest-original-api original-sql/test-boolean-cast)
(deftest-original-api original-sql/can-not-write-to-reserved-tables)
(deftest-original-api original-sql/test-keyword)
(deftest-original-api original-sql/test-str)
(deftest-original-api original-sql/select-with-lots-of-commas)
(deftest-original-api original-sql/null-valid-time-insertion-behaves-as-if-non-specified)
(deftest-original-api original-sql/forbid-invalid-columns-update)

(deftest-original-api original-update/update-changes-types-if-requested-5030)

(deftest-original-api original-update/update-doesnt-put-unchanged-versions-5030)

(deftest-original-api original-update/update-for-portion-of-idempotency-5030)
(deftest-original-api original-sql/joins-across-polymorphic-data-4061+4078)
(deftest-original-api original-sql/test-hashcode-for-tstzrange-4263)
(deftest-original-api original-sql/patch-uuid-literal-id-4284)
(deftest-original-api original-sql/interval-mdm)
(deftest-original-api original-sql/inconsistent-patch-behaviour-4448)
(deftest-original-api original-sql/temporal-filter-expressions-3306)
(deftest-original-api original-sql/order-by-without-select-bug-4560)
(deftest-original-api original-sql/update-with-many-params-4803)
(deftest-original-api original-sql/scrambled-update-4803)
(deftest-original-api original-sql/subquery-in-exists-incorrect-table-name-4697)
(deftest-original-api original-sql/update-interval-with-months-5070)
(deftest-original-api original-sql/update-dedup-structs-5113)
(deftest-original-api original-sql/test-or-on-id)
(deftest-original-api original-expr/test-round-type-preservation)
(deftest-original-api original-expr/test-round-precision-fix)
(deftest-original-api original-expr/test-round-half-up-mode)
(deftest-original-api original-expr/test-round-decimal-constraints)
(deftest-original-api original-expr/test-interval-comparison)
(deftest-original-api original-expr/empty-array-column-compares-5948)
(deftest-original-api original-expr/test-cast-temporal-to-string)
(deftest-original-api original-expr/test-cast-interval-to-duration)
(deftest-original-api original-expr/test-cast-duration-to-interval)
(deftest-original-api original-expr/test-cast-interval-to-interval)
(deftest-original-api original-expr/test-cast-int-to-interval)
(deftest-original-api original-expr/test-cast-string-to-interval-without-qualifier)
(deftest-original-api original-expr/test-cast-interval-to-string)
(deftest-original-api original-expr/test-cast-nested-interval-to-string-prints-interval)
(deftest-original-api original-expr/test-current-setting)
(deftest-original-api original-expr/test-date-trunc-query)
(deftest-original-api original-expr/test-date-trunc-with-timezone-query)
(deftest-original-api original-expr/test-date-trunc-with-interval-query)
(deftest-original-api original-expr/test-date-bin)
(deftest-original-api original-expr/test-range-bins)
(deftest-original-api original-expr/test-extract-query)
(deftest-original-api original-expr/test-age-function)
(deftest-original-api original-expr/test-lower-upper-period-fns-3660)
(deftest-original-api original-expr/test-quote-ident)
(deftest-original-api original-expr/test-parse-ident)
(deftest-original-api original-expr/test-arithmetic-precedence-slt)
(deftest-original-api original-expr/timestamp-literal-exception-3562)
(deftest-original-api original-expr/timestamptz-literals-and-casts-3612)

(deftest-original-api original-expr/test-cast-string-to-temporal)
(deftest-original-api original-expr/test-cast-string-to-interval-with-qualifier)

(deftest-original-api original-expr/test-implicit-string-literal-to-temporal-coercion-in-comparisons)
(deftest-original-api original-expr/test-interval-past-interval-range-raises-overflow-error)
(deftest-original-api original-expr/test-timezone-single-word-syntax)
(deftest-original-api original-expr/variadic-overlaps-3441)

(deftest-original-api original-precedence/multiplication-binds-tighter-than-addition)
(deftest-original-api original-precedence/bitwise-operators-share-a-level-and-associate-left)
(deftest-original-api original-precedence/element-access-binds-tighter-than-arithmetic)
(deftest-original-api original-precedence/predicates-bind-tighter-than-comparisons)
(deftest-original-api original-precedence/comparisons-bind-tighter-than-is)
(deftest-original-api original-precedence/boolean-operators-bind-loosest)
(deftest-original-api original-exists/decorrelated-exists-returns-the-rows-the-apply-plan-returned)
(deftest-original-api original-correlated/not-exists-ignores-inner-rows-its-where-filters-out-6313)
(deftest-original-api original-correlated/exists-in-a-projection-is-false-when-its-where-filters-out-every-row-6313)
(deftest-original-api original-correlated/correlated-in-is-false-when-its-where-filters-out-every-row-6313)
(deftest-original-api original-correlated/not-in-stays-unknown-against-a-null-the-where-keeps)
(deftest-original-api original-interval/test-interval-literals)
(deftest-original-api original-interval/test-interval-fns)
(deftest-original-api original-temporal/all-system-time)
(deftest-original-api original-temporal/test-inconsistent-valid-time-range-2494)
(deftest-original-api original-temporal/test-period-intersection-3493)
(deftest-original-api original-temporal/update-delete-and-patch-explicit-null-behaviour)
(deftest-original-api original-temporal/from-explicit-null-behaviour)
(deftest-original-api original-temporal/no-portion-of-valid-time-dml-4650)
(deftest-original-api original-temporal/patch-gaps-filling-test-4649)
(deftest-original-api original-temporal/duration-abs-sql-test)
(deftest-original-api original-with/cte-exporting-valid-time-4054)
(deftest-original-api original-with/test-materialized-cte-ordering-with-many-ctes-5109)
(deftest-original-api original-series/test-range-3212 series-test-range-3212)
(deftest-original-api original-series/test-range-datetimes-4067)
(deftest-original-api original-series/test-generate-series-3212 series-test-generate-series-3212)
(deftest-original-api original-series/test-negative-step)
(deftest-original-api original-series/test-generate-series-datetimes-4067)
(deftest-original-api original-series/test-generate-series-limit-batching-4412)
(deftest-original-api original-identifiers/semi-reserved-keywords-are-usable-as-an-as-alias-6014)
(deftest-original-api original-identifiers/semi-reserved-keywords-are-usable-as-a-derived-table-column-alias-6014)

(deftest-original-api original-update/update-bulk-5030)

(deftest-original-api original-node/test-multi-value-insert-423)

(deftest-original-api original-node/test-dml-sees-in-tx-docs)

(deftest-original-api original-node/test-delete-without-search-315)

(deftest-original-api original-node/test-update-set-field-from-param-328)

(deftest-original-api original-node/test-put-delete-with-implicit-tables-338)

(deftest-original-api original-node/test-interval-literal-cce-271)

(deftest-original-api original-node/test-overrides-range)

(deftest-original-api original-node/test-current-timestamp-in-temporal-constraint-409)

(deftest-original-api original-node/test-vector-type-mismatch-245)

(deftest-original-api original-node/test-double-quoted-col-refs)

(deftest-original-api original-node/test-select-left-join-2302)

(deftest-original-api original-node/test-list-round-trip-2342)

(deftest-original-api original-issue/test-nested-lists-3341)

(deftest-original-api original-info-schema/columns-data-type-follows-postgres-spelling)

(deftest test-pg-attribute
  (binding [original-info-schema/*source-log-time-column?* true]
    (run-original-api-case #'original-info-schema/test-pg-attribute)))

(deftest-original-api original-node/test-mutable-data-buffer-bug)

(deftest-original-api original-node/test-differing-length-lists-441)

(deftest-original-api original-node/test-normalising-nested-cols-2483)

(deftest-original-api original-node/non-namespaced-keys-for-structs-2418)

(deftest-original-api original-node/large-xt-stars-2484)

(deftest-original-api original-node/log-backpressure-doesnt-halt-indexer-4285)

(deftest wide-sparse-document-transactions-commit-and-retain-their-declared-columns
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (let [writes (for [id (range 100)]
                   (let [doc (into {:xt/id id}
                                   (for [column (range 10)]
                                     [(keyword (str "field_" id "_" column))
                                      (str "value_" id "_" column)]))]
                     [:put-docs :docs (cond-> doc (zero? id) (assoc :all_null nil))]))]
      (is (= 0 (:tx-id (xt/execute-tx node writes))))
      (is (= [{:field-99-9 "value_99_9"}]
             (xt/q node "SELECT field_99_9, field_0_0, all_null FROM docs WHERE _id = 99"))))))

(deftest-original-api original-node/non-existant-column-no-nil-rows-2898)

(deftest-original-api original-node/test-get-field-on-duv-with-struct-2425)

(deftest-original-api original-node/distinct-null-2535)

(deftest-original-api original-node/throw-on-unknown-query-type)

(deftest-original-api original-node/test-array-agg-2946)

(deftest-original-api original-node/test-ts-tz-zone-first-wins-3723)

(deftest-original-api original-node/able-to-override-valid-time-in-put-docs)

(deftest-original-api original-node/ingestion-stopped-on-null-col-ref-4259)

(deftest-original-api original-node/test-merge-fields-npe-4721)

(deftest-original-api original-node/test-info-schema-with-empty-set-children-4774)

(deftest-original-api original-node/test-string-escaping-in-vectors)

(deftest-original-api original-patch/test-patch-3879)

(deftest-original-api original-patch/test-patch-operator)

(deftest-original-api original-patch/test-multi-entity-patch-sql)

(deftest-original-api original-patch/test-patch-sql)

(deftest-original-api original-patch/patching-lists-with-nils-4752)

(deftest-original-api original-patch/test-patch-list-of-structs-with-nils)

(deftest-original-api original-percentile/test-percentile-via-sql)

(deftest-original-api original-percentile/test-percentile-cont-basic)

(deftest-original-api original-percentile/test-percentile-cont-interpolation)

(deftest-original-api original-percentile/test-percentile-cont-single-value)

(deftest-original-api original-percentile/test-percentile-cont-with-nulls)

(deftest-original-api original-percentile/test-percentile-cont-empty)

(deftest-original-api original-percentile/test-percentile-cont-desc)

(deftest-original-api original-percentile/test-percentile-cont-grouped)

(deftest-original-api original-percentile/test-percentile-disc-basic)

(deftest-original-api original-percentile/test-percentile-disc-desc)

(deftest-original-api original-percentile/test-percentile-disc-grouped)

(deftest-original-api original-percentile/test-percentile-disc-strings)

(deftest-original-api original-percentile/test-percentile-cont-float-input)

(deftest-original-api original-percentile/test-percentile-cont-all-nulls)

(deftest-original-api original-percentile/test-percentile-across-batches)

(deftest logical-percentile-uses-named-fraction-and-filter
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (is (= [{:p 30.0}]
           (rust/query-ra node
                          '[:group-by {:columns [{p (percentile_cont ?fraction [v {:direction :asc}] {:filter keep})}]}
                            [:table {:rows [{:v 10 :keep true} {:v 30 :keep true} {:v 50 :keep true} {:v 70 :keep false}]}]]
                          {:args {:fraction 0.5}})))))

(deftest-original-api original-unnest/test-vector-not-found-4343)

(deftest-original-api original-unnest/test-simple-unnest)

(deftest-original-api original-unnest/test-unnest-operator)

(deftest-original-api original-unnest/fix-ordinality-field-naming)

(deftest-original-api original-unnest/unnest-issue-4441)

(deftest-original-api original-list/test-generate-series)

(deftest-original-api original-list/test-batch-boundaries)

(deftest-original-api original-unnest/unnest-ordinality-join-issue-4131)

(deftest-original-api original-scan/live-hash-trie-branches-get-expanded-3247)

(deftest-original-api original-scan/test-simple-scan)

(deftest-original-api original-scan/test-simple-scan-with-namespaced-attributes)

(deftest-original-api original-scan/test-duplicates-in-scan-1)

(deftest-original-api original-scan/test-iid-col-type-3016)

(deftest-original-api original-scan/dont-use-fast-path-on-non-literals-2768)

(deftest-original-api original-scan/test-past-point-point-queries)

(deftest-original-api original-scan/test-past-point-point-queries-with-valid-time)

(deftest-original-api original-scan/test-scanning-temporal-cols)

(deftest-original-api original-scan/test-only-scanning-temporal-cols-45)

(deftest-original-api original-scan/test-for-valid-time-in-params)

(deftest-original-api original-scan/test-content-pred)

(deftest-original-api original-scan/test-absent-columns)

(deftest-original-api original-scan/test-aligns-temporal-columns-correctly-363)

(deftest-original-api original-scan/test-scan-col-types)

(deftest-original-api original-scan/test-pushdown-blooms-through-project)

(deftest logical-explain-plan-reports-the-native-scan
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node [[:put-docs :docs {:xt/id 1, :v 42}]])
    (let [rows (rust/query-ra node '[:scan {:db-name "xtdb", :table #xt/table docs, :columns [v]}]
                              {:explain? true})]
      (is (= [:scan] (mapv :op rows))))))

(deftest-original-api original-as-of/test-as-of-tx)

(deftest-original-api original-as-of/test-app-time)

(deftest-original-api original-pg-expression/test-col-description)

(deftest-original-api original-pg-expression/test-json-arrow-operator)

(deftest-original-api original-pg-expression/test-json-arrow-text-operator)

(deftest-original-api original-pg-expression/test-json-path-operator)

(deftest-original-api original-pg-expression/test-json-path-text-operator)

(deftest-original-api original-pg-expression/test-transit-field-access)

(deftest-original-api original-pg-expression/test-format-type)

(deftest unnest-actor-results
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/submit-tx node [[:put-docs :actors
                        {:xt/id "bob", :name "Bob", :films ["The Great Escape", "Bob the Builder", "Bob the Builder Electric Boogaloo"]}
                        {:xt/id "steve", :name "Steve", :films ["Hot Fuzz" "Shaun of the Dead"]}]])

    (is (= [{:name "Bob", :film "The Great Escape"}
            {:name "Bob", :film "Bob the Builder"}
            {:name "Bob", :film "Bob the Builder Electric Boogaloo"}
            {:name "Steve", :film "Hot Fuzz"}
            {:name "Steve", :film "Shaun of the Dead"}]
           (xt/q node "SELECT name, film FROM actors, UNNEST(films) AS films(film)")))

    (is (= [{:name "Bob", :film "The Great Escape", :ord 1}
            {:name "Bob", :film "Bob the Builder", :ord 2}
            {:name "Bob", :film "Bob the Builder Electric Boogaloo", :ord 3}
            {:name "Steve", :film "Hot Fuzz", :ord 1}
            {:name "Steve", :film "Shaun of the Dead", :ord 2}]
           (xt/q node "SELECT name, film, ord FROM actors, UNNEST(films) WITH ORDINALITY AS films(film, ord)")))))

(deftest unnest-null-or-missing-4075
  (run-original-api-case #'original-sql/test-unnest-null-or-missing-4075))

(deftest-original-api original-group-by/test-min-max-over-declared-empty-column-return-nil-6133)

(deftest-original-api original-group-by/test-min-max-over-declared-column-absent-from-written-rows-return-nil-6133)

(deftest incompatible-extrema-retain-the-vector-type-in-transit
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (let [error (try
                  (xt/q node "SELECT MAX(v) AS m FROM (VALUES (12), ('twelve')) AS t(v)")
                  (catch xtdb.api.error.Incorrect error error))]
      (is (instance? xtdb.api.error.Incorrect error))
      (is (= :xtdb.group-by/incomparable-min-max-types (:xtdb.error/code (ex-data error))))
      (is (instance? xtdb.arrow.VectorType (:from-type (ex-data error)))))))
