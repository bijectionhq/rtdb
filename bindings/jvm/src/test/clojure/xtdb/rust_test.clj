(ns xtdb.rust-test
  (:require [clojure.test :as t :refer [deftest is]]
            [xtdb.api :as xt]
            [xtdb.operator.group-by-test :as original-group-by]
            [xtdb.sql.expr-test :as original-expr]
            [xtdb.sql-test :as original-sql]
            [xtdb.test-util :as tu]
            [xtdb.protocols :as protocols]
            [xtdb.rust])
  (:import [java.time InstantSource]
           [org.apache.arrow.memory RootAllocator]
           [xtdb.rust NativeDatabase]))

(def ^:private node-config "server: null\nflightSql: null\nnodeId: clojure-binding-test\n")

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

(deftest api-adbc-test-preserves-nil-vs-present-empty-sql-batch
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node ["INSERT INTO docs (_id) VALUES (1)"])
    (xt/execute-tx node [["INSERT INTO docs (_id) VALUES (?)" 2]])
    (protocols/-execute-tx node [{:op :sql, :sql "INSERT INTO docs (_id) VALUES (3)", :arg-rows []}] {})
    (is (= [{:xt/id 1} {:xt/id 2}] (xt/q node "SELECT _id FROM docs ORDER BY _id")))))

(defn- with-native-node [f]
  (let [[log-type {:keys [instant-src]}] (:log tu/*node-opts*)]
    (with-open [node (case log-type
                      nil (open-node tu/*allocator*)
                      :in-memory (open-node tu/*allocator* instant-src)
                      (throw (ex-info "Unsupported native log fixture" {:log-type log-type})))]
      (binding [tu/*node* node]
        (f)))))

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
    ((t/join-fixtures native-fixtures) (:test (meta original)))))

(defmacro ^:private deftest-original-api [original]
  `(deftest ~(symbol (name original))
     (run-original-api-case (var ~original))))

(deftest-original-api original-sql/multiple-query-tails)

(deftest-original-api original-expr/duration-literal-query)

(deftest-original-api original-sql/plan-agg-subqueries-before-group-by-3821)

(deftest-original-api original-sql/test-select-without-from)

(deftest-original-api original-sql/test-generated-column-names)

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

(deftest-original-api original-sql/test-current-catalog)
(deftest-original-api original-sql/test-delete-with-unqualified-col-3017)
(deftest-original-api original-sql/forbid-updating-core-cols-3433)

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
