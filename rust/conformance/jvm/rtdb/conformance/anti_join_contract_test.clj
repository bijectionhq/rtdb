(ns rtdb.conformance.anti-join-contract-test
  (:require [clojure.test :as t]
            [jsonista.core :as json]
            [xtdb.error :as err]
            [xtdb.operator.join :as join]
            [xtdb.test-util :as tu])
  (:import [xtdb.api.error Incorrect]
           [xtdb.arrow IntVector LongVector RelationReader]
           [xtdb.expression.map IndexHasher]))

(def ^:dynamic *cases*)

(defn- join-key-hashes [probe-key build-key]
  (with-open [keys (LongVector. tu/*allocator* "key" false)
              hashes (IntVector/open tu/*allocator* "hashes" false)
              scratch (IntVector/open tu/*allocator* "scratch" false)]
    (.writeLong keys (long probe-key))
    (.writeLong keys (long build-key))
    (.hashTo (IndexHasher. (RelationReader/from [keys] 2) [keys]) hashes scratch)
    [(.getInt hashes 0) (.getInt hashes 1)]))

(defn- observe [{[probe-key probe-input probe-id] :probe, :keys [build]}]
  (let [probe-rows [{:probe-key probe-key, :probe-input probe-input, :probe-id probe-id}]
        build-rows (mapv (fn [[key input]] {:build-key key, :build-input input}) build)]
    (try
      {:rows (mapv :probe-id
                   (tu/query-ra [:anti-join '{:conditions [{probe-key build-key}
                                                           (> (/ 1.0 (+ probe-input build-input)) 0.0)]}
                                 [:table {:rows probe-rows}]
                                 [:table {:rows build-rows}]]))}
      (catch Incorrect e
        {:error (some-> (::err/code (ex-data e)) str (subs 1))}))))

(t/deftest anti-join-probes-only-full-hash-candidates-of-nonnull-keys
  (tu/with-allocator
    (fn []
      (doseq [{:keys [id reference probe build first_build_key_collides] :as scenario} *cases*]
        (when first_build_key_collides
          (let [probe-key (first probe)
                first-build-key (ffirst build)
                [probe-hash build-hash] (join-key-hashes probe-key first-build-key)]
            (t/is (not= probe-key first-build-key) id)
            (t/is (= probe-hash build-hash) id)))
        (doseq [[mode threshold] [[:memory Long/MAX_VALUE] [:spill 1]]]
          (binding [join/*disk-join-threshold-rows* threshold]
            (t/is (= reference (observe scenario)) (str id " " mode))))))))

(defn -main [fixture-path]
  (binding [*cases* (json/read-value (slurp fixture-path) json/keyword-keys-object-mapper)]
    (let [{:keys [fail error]} (t/run-tests 'rtdb.conformance.anti-join-contract-test)]
      (when (pos? (+ fail error)) (System/exit 1)))))
