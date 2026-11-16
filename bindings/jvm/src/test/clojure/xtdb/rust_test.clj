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
