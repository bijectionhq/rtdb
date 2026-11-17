(ns rtdb.conformance.history-test
  (:require [clojure.test :as test :refer [deftest is]]
            [rtdb.conformance.history :as history])
  (:import (java.lang.reflect InvocationHandler Method Proxy)
           (java.nio.file Path)
           (org.apache.arrow.vector.ipc.message ArrowBlock ArrowFooter ArrowRecordBatch)
           (org.apache.arrow.vector.types.pojo ArrowType$Int Schema)
           (xtdb.arrow VectorType$Scalar)
           (xtdb.storage BufferPool)))

(set! *warn-on-reflection* true)

(defn- fake-pool ^BufferPool [calls]
  (Proxy/newProxyInstance
    (.getClassLoader ^Class BufferPool) (into-array Class [BufferPool])
    (reify InvocationHandler
      (invoke [_ _ method _]
        (swap! calls conj (.getName ^Method method))
        (case (.getName ^Method method)
          "getByteArray" (byte-array [1 2 3])
          "close" nil
          (throw (ex-info "Read failed" {})))))))

(deftest successful-byte-array-reads-count-once-and-wrapper-close-preserves-borrowed-owner
  (let [calls (atom []) counts (atom [0 0])
        pool (history/counting-pool (fake-pool calls) counts)]
    (is (= 3 (alength (.getByteArray pool (Path/of "key" (make-array String 0))))))
    (is (= [3 1] @counts))
    (.close pool)
    (is (= ["getByteArray"] @calls))))

(deftest failed-reads-and-rejected-mutations-do-not-count
  (let [calls (atom []) counts (atom [0 0])
        pool (history/counting-pool (fake-pool calls) counts)]
    (is (thrown? Exception (.getFooter pool (Path/of "key" (make-array String 0)))))
    (is (thrown? Exception (.deleteIfExistsSync pool (Path/of "key" (make-array String 0)))))
    (is (= [0 0] @counts))
    (is (= ["getFooter"] @calls))))

(deftest synchronous-and-suspend-entrypoints-count-one-successful-span-each
  (let [calls (atom []) counts (atom [0 0])
        delegate (Proxy/newProxyInstance
                   (.getClassLoader ^Class BufferPool) (into-array Class [BufferPool])
                   (reify InvocationHandler
                     (invoke [_ _ method _]
                       (swap! calls conj (.getName ^Method method))
                       (case (.getName ^Method method)
                         "getFooter" (ArrowFooter. (Schema. []) [] [(ArrowBlock. 0 8 24)])
                         "getRecordBatchSync" (ArrowRecordBatch. 0 [] [])
                         (throw (ex-info "Unexpected delegate method" {}))))))
        pool (history/counting-pool delegate counts)
        path (Path/of "key" (make-array String 0))]
    (with-open [batch (.getRecordBatchSync pool path 0)]
      (is (= [32 1] @counts)))
    (with-open [^ArrowRecordBatch batch (.getRecordBatch pool path 0 nil)]
      (is (= [64 2] @counts)))
    (is (= ["getFooter" "getRecordBatchSync" "getFooter" "getRecordBatchSync"] @calls))))

(deftest canonical-projection-keeps-int64-type-and-rejects-narrower-integers
  (is (= [{:kind "i64"}] (history/type-tags (VectorType$Scalar. (ArrowType$Int. 64 true)))))
  (is (thrown? Exception (history/type-tags (VectorType$Scalar. (ArrowType$Int. 32 true))))))

(deftest fixture-validation-rejects-ambiguous-identities-before-opening-nodes
  (let [fixture {:schema_version 1 :kind "historical_prices" :timezone "UTC"
                 :transactions [{:operation "put" :system_time "2026-04-01T00:00:00Z" :valid_from "2026-04-01T00:00:00Z" :prices {:A 12}}]
                 :prefixes [{:id "april" :transactions 1}]
                 :queries [{:id "price" :prefix "april" :valid_time "2026-04-01T00:00:00Z" :system_time "2026-04-01T00:00:00Z"}]}]
    (is (= fixture (history/validate-fixture fixture)))
    (is (thrown? Exception (history/validate-fixture (update fixture :queries #(conj % (first %))))))
    (is (thrown? Exception (history/validate-fixture (assoc-in fixture [:prefixes 0 :id] "../escape"))))
    (is (thrown? Exception (history/validate-fixture (assoc-in fixture [:prefixes 0 :transactions] 2))))))

(defn -main [& _]
  (let [{:keys [fail error]} (test/run-tests 'rtdb.conformance.history-test)]
    (when (pos? (+ (long fail) (long error)))
      (throw (ex-info "History adapter source tests failed" {:fail fail :error error})))))
