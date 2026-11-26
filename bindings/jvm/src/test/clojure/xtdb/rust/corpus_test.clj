(ns xtdb.rust.corpus-test
  (:require [clojure.java.io :as io]
            [clojure.test :as t]
            [xtdb.rust.corpus :as corpus])
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
