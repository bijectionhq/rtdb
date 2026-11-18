(ns xtdb.rust-test
  (:require [clojure.test :refer [deftest is]]
            [xtdb.api :as xt]
            [xtdb.protocols :as protocols]
            [xtdb.rust])
  (:import [org.apache.arrow.memory RootAllocator]
           [xtdb.rust NativeDatabase]))

(defn- open-node ^NativeDatabase [allocator]
  (NativeDatabase/open allocator "server: null\nflightSql: null\nnodeId: clojure-binding-test\n" (* 512 1024 1024)))

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

(deftest multiple-query-tails
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/submit-tx node [[:put-docs :foo {:xt/id 1, :v 1}]
                        [:put-docs :foo {:xt/id 2, :v 1}]
                        [:put-docs :foo {:xt/id 3, :v 2}]
                        [:put-docs :foo {:xt/id 4, :v 3}]
                        [:put-docs :foo {:xt/id 5, :v 4}]])

    (is (= [{:v 1, :v-count 2} {:v 2, :v-count 1} {:v 3, :v-count 1} {:v 4, :v-count 1}]
           (xt/q node "FROM foo
                            SELECT v, COUNT(*) AS v_count
                            ORDER BY v")))

    (is (= [{:v-count 1, :freq 3} {:v-count 2, :freq 1}]
           (xt/q node "FROM foo
                            SELECT v, COUNT(*) AS v_count
                            SELECT v_count, COUNT(*) AS freq
                            ORDER BY v_count"))
        "double aggregate")

    (is (= [{:v 2} {:v 3} {:v 4} {:v 1}]
           (xt/q node "FROM foo
                            SELECT v, COUNT(*) AS v_count
                            SELECT v
                            ORDER BY v_count, v"))
        "ORDER BY sees through outermost SELECT")

    (is (= [{:xt/id 1} {:xt/id 3} {:xt/id 4} {:xt/id 5}]
           (xt/q node "FROM foo
                            SELECT _id, ROW_NUMBER () OVER (PARTITION BY v ORDER BY _id) row_num
                            WHERE row_num = 1
                            SELECT _id
                            ORDER BY _id"))
        "we can filter on window functions")))

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
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node ["CREATE TABLE foo (b)"])
    (xt/execute-tx node ["INSERT INTO foo RECORDS {_id: 1, a: 2}"])
    (is (= [] (xt/q node "SELECT bs.b FROM foo, UNNEST(foo.b) AS bs(b)")))))

(deftest test-min-max-over-declared-empty-column-return-nil-6133
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node [[:sql "CREATE TABLE docs (v)"]])

    (is (= [{}]
           (xt/q node "SELECT MIN(v) AS mn, MAX(v) AS mx FROM docs"))
        "directly over the scan")

    (is (= [{}]
           (xt/q node "SELECT MIN(v) AS mn, MAX(v) AS mx
                            FROM (SELECT v FROM docs UNION ALL SELECT v FROM docs) t"))
        "over a UNION ALL of the scan")))

(deftest test-min-max-over-declared-column-absent-from-written-rows-return-nil-6133
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (xt/execute-tx node [[:sql "CREATE TABLE docs (v)"]
                         [:put-docs :docs {:xt/id 1}]])

    (is (= [{}]
           (xt/q node "SELECT MIN(v) AS mn, MAX(v) AS mx
                            FROM (SELECT v FROM docs UNION ALL SELECT v FROM docs) t")))))

(deftest incompatible-extrema-retain-the-vector-type-in-transit
  (with-open [allocator (RootAllocator.)
              node (open-node allocator)]
    (let [error (try
                  (xt/q node "SELECT MAX(v) AS m FROM (VALUES (12), ('twelve')) AS t(v)")
                  (catch xtdb.api.error.Incorrect error error))]
      (is (instance? xtdb.api.error.Incorrect error))
      (is (= :xtdb.group-by/incomparable-min-max-types (:xtdb.error/code (ex-data error))))
      (is (instance? xtdb.arrow.VectorType (:from-type (ex-data error)))))))
