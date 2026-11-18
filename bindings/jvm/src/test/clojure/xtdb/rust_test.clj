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

(deftest multiple-query-tails
  (run-original-api-case #'original-sql/multiple-query-tails))

(deftest duration-literal-query
  (run-original-api-case #'original-expr/duration-literal-query))

(deftest plan-agg-subqueries-before-group-by-3821
  (run-original-api-case #'original-sql/plan-agg-subqueries-before-group-by-3821))

(deftest test-select-without-from
  (run-original-api-case #'original-sql/test-select-without-from))

(deftest test-generated-column-names
  (run-original-api-case #'original-sql/test-generated-column-names))

(deftest test-empty-array-3818
  (run-original-api-case #'original-sql/test-empty-array-3818))

(deftest test-function-as-from-source
  (run-original-api-case #'original-sql/test-function-as-from-source))

(deftest test-range-3212
  (run-original-api-case #'original-sql/test-range-3212))

(deftest test-generate-series-3212
  (run-original-api-case #'original-sql/test-generate-series-3212))

(deftest test-order-nulls-first-last
  (run-original-api-case #'original-sql/test-order-nulls-first-last))

(deftest test-xt-version
  (run-original-api-case #'original-sql/test-xt-version))

(deftest test-bare-user-keyword
  (run-original-api-case #'original-expr/test-bare-user-keyword))

(deftest test-like-regex-query-case-insensitive
  (run-original-api-case #'original-expr/test-like-regex-query-case-insensitive))

(deftest test-ilike-query
  (run-original-api-case #'original-expr/test-ilike-query))

(deftest test-postgres-regex-query
  (run-original-api-case #'original-expr/test-postgres-regex-query))

(deftest test-reverse-query
  (run-original-api-case #'original-expr/test-reverse-query))

(deftest test-concat-function-query
  (run-original-api-case #'original-expr/test-concat-function-query))

(deftest test-position-with-substring
  (run-original-api-case #'original-expr/test-position-with-substring))

(deftest test-substring-for-length-on-nested-substring
  (run-original-api-case #'original-expr/test-substring-for-length-on-nested-substring))

(deftest test-length-query
  (run-original-api-case #'original-expr/test-length-query))

(deftest test-replace
  (run-original-api-case #'original-expr/test-replace))

(deftest interval-literal-query
  (run-original-api-case #'original-expr/interval-literal-query))

(deftest test-postgres-session-information-functions
  (run-original-api-case #'original-expr/test-postgres-session-information-functions))

(deftest test-string-to-array
  (run-original-api-case #'original-expr/test-string-to-array))

(deftest test-array-upper
  (run-original-api-case #'original-expr/test-array-upper))

(deftest test-array-lower
  (run-original-api-case #'original-expr/test-array-lower))

(deftest test-array-length
  (run-original-api-case #'original-expr/test-array-length))

(deftest test-uri-fns
  (run-original-api-case #'original-expr/test-uri-fns))

(deftest period-nulls-4315
  (run-original-api-case #'original-expr/period-nulls-4315))

(deftest test-lateral-with-unnest-4009
  (run-original-api-case #'original-sql/test-lateral-with-unnest-4009))

(deftest interval-read-write-bug-4066
  (run-original-api-case #'original-sql/interval-read-write-bug-4066))

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

(deftest test-min-max-over-declared-empty-column-return-nil-6133
  (run-original-api-case #'original-group-by/test-min-max-over-declared-empty-column-return-nil-6133))

(deftest test-min-max-over-declared-column-absent-from-written-rows-return-nil-6133
  (run-original-api-case #'original-group-by/test-min-max-over-declared-column-absent-from-written-rows-return-nil-6133))

(deftest incompatible-extrema-retain-the-vector-type-in-transit
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (let [error (try
                  (xt/q node "SELECT MAX(v) AS m FROM (VALUES (12), ('twelve')) AS t(v)")
                  (catch xtdb.api.error.Incorrect error error))]
      (is (instance? xtdb.api.error.Incorrect error))
      (is (= :xtdb.group-by/incomparable-min-max-types (:xtdb.error/code (ex-data error))))
      (is (instance? xtdb.arrow.VectorType (:from-type (ex-data error)))))))
