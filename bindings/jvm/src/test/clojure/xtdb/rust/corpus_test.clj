(ns xtdb.rust.corpus-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.api :as xt]
            [xtdb.rust.corpus :as corpus]
            [xtdb.test-util :as tu])
  (:import [java.net URL URLClassLoader]
           [java.nio.file Files]
           [java.nio.file.attribute FileAttribute]))

(defn- temp-dir []
  (.toFile (Files/createTempDirectory "corpus-test" (make-array FileAttribute 0))))

(t/deftest a-var-records-every-failure-not-only-the-first
  (let [probe-ns (create-ns 'xtdb.rust.corpus-test.probe)
        v (intern probe-ns (with-meta 'two-failures {:test (fn []
                                                             (t/is (= 1 2))
                                                             (t/is true)
                                                             (t/is (= 3 4)))}))
        {:keys [result failures detail pass fail] :as row} (corpus/run-var v 10000)]
    (t/is (= "fail" result) (pr-str row))
    (t/is (= [1 2] [pass fail]))
    (t/is (= 2 (count failures)))
    (t/is (= (first failures) detail))
    (t/is (re-find #"\(= 3 4\)" (second failures)))
    (t/is (not (:failures-truncated row)))))

(t/deftest a-namespace-loads-from-the-root-that-holds-its-source
  (let [root (temp-dir)
        elsewhere (temp-dir)
        source (io/file root "xtdb" "corpus_probe_test.clj")
        thread (Thread/currentThread)
        original (.getContextClassLoader thread)]
    (io/make-parents source)
    (spit source "(ns xtdb.corpus-probe-test)")
    (.setContextClassLoader thread (URLClassLoader. (into-array URL [(.toURL (.toURI root))]) original))
    (try
      (t/is (corpus/loads-from? root 'xtdb.corpus-probe-test))
      (t/is (not (corpus/loads-from? elsewhere 'xtdb.corpus-probe-test)))
      (t/is (not (corpus/loads-from? root 'xtdb.corpus-probe-absent-test)))
      (finally
        (.setContextClassLoader thread original)))))

(t/deftest flight-port-adapter-preserves-server-options-and-assertions
  (let [adapt #'corpus/adapt-listener-ports
        source "(t/is (anomalous? [:incorrect :xtdb/unknown-db]
                  (fsql/open-conn {:port (.getFlightSqlPort ^Xtdb *node*), :dbname \"no-such-db\"})))
                (fsql/open-conn {:port (.getFlightSqlPort ^Xtdb *node*)})
                (fsql/open-conn {:port (.getFlightSqlPort node)})
                (fsql/open-conn {:port (.getFlightSqlPort node)})"]
    (t/is (= "(t/is (anomalous? [:incorrect :xtdb/unknown-db]
                  (fsql/open-conn {:port (xtdb.rust.corpus/native-listener-port *node* :flight-sql), :dbname \"no-such-db\"})))
                (fsql/open-conn {:port (xtdb.rust.corpus/native-listener-port *node* :flight-sql)})
                (fsql/open-conn {:port (xtdb.rust.corpus/native-listener-port node :flight-sql)})
                (fsql/open-conn {:port (xtdb.rust.corpus/native-listener-port node :flight-sql)})"
             (adapt 'xtdb.api-flight-sql-test source)))
    (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"Unexpected upstream listener port call sites"
                            (adapt 'xtdb.api-flight-sql-test "")))))

(t/deftest a-corpus-namespace-publishes-its-vars-before-outcomes
  (let [root (temp-dir)
        source (io/file root "xtdb/corpus_order_test.clj")
        thread (Thread/currentThread)
        original (.getContextClassLoader thread)
        seen (atom [])]
    (io/make-parents source)
    (spit source "(ns xtdb.corpus-order-test (:require [clojure.test :as t]))
                  (t/deftest first-case (t/is false))
                  (t/deftest ^:integration second-case (t/is false))")
    (with-open [loader (URLClassLoader. (into-array URL [(.toURL (.toURI root))]) original)]
      (.setContextClassLoader thread loader)
      (try
        (t/is (true? (corpus/run-namespace root 'xtdb.corpus-order-test 10000 #(swap! seen conj %))))
        (t/is (= {:event "namespace-start" :vars ["xtdb.corpus-order-test/first-case" "xtdb.corpus-order-test/second-case"]}
                 (first @seen)))
        (t/is (= ["fail" "skipped-integration"] (mapv :result (subvec @seen 1 3))))
        (t/is (= {:event "namespace-complete"} (peek @seen)))
        (finally
          (.setContextClassLoader thread original))))))

(t/deftest corpus-fixture-reads-latest-completed-tx-from-native-node
  (#'corpus/with-native-engine
   (fn []
     (with-open [node (corpus/native-start-node {:server nil :flight-sql nil})]
       (t/is (= {"xtdb" [nil]} (tu/latest-completed-txs node)))
       (let [tx (xt/execute-tx node [[:put-docs :docs {:xt/id 1}]])]
         (t/is (= {"xtdb" [tx]} (tu/latest-completed-txs node))))))))
