(ns xtdb.rust.property-corpus-test
  (:require [clojure.test :as t]
            [clojure.test.check.clojure-test :as tct]
            [clojure.test.check.generators :as gen]
            [clojure.test.check.properties :as prop]
            [clojure.test.check.rose-tree :as rose]
            [xtdb.expression-test :as expression]
            [xtdb.rust.property-corpus :as corpus]
            [xtdb.test-util :as tu])
  (:import [xtdb.time Interval]))

(tct/defspec captures-defspec-without-running-its-predicate 1
  (prop/for-all [n (gen/return 11)] (= n 11)))

(t/deftest ^:property captures-deftest-without-running-its-predicate
  (tu/run-property-test
   {:num-tests 1}
   (prop/for-all [n (gen/return 12)] (= n 12))))

(t/deftest original-property-entry-points-retain-generator-and-predicate
  (doseq [[v n] [[#'captures-defspec-without-running-its-predicate 11]
                 [#'captures-deftest-without-running-its-predicate 12]]]
    (let [{:keys [generators predicate]} (corpus/capture-property v)]
      (t/is (= [n] (mapv #(gen/generate % 0 42) generators)))
      (t/is (true? (predicate n)))
      (t/is (false? (predicate (inc n)))))))

(def ^:private settings {:seed 41, :trials 4, :max_size 8})

(t/deftest replay-runs-the-first-and-smallest-counterexamples-through-native-projection
  (let [inputs (volatile! [])
        property {:generators [(gen/->Generator (fn [_ _] (rose/make-rose 90 [(rose/pure 3) (rose/pure 10)])))]
                  :predicate (fn [n]
                               (vswap! inputs conj n)
                               (= n (expression/project1 '(character-length a) {:a "€☃😀"})))}]
    (corpus/with-observers
      "defspec"
      (fn []
        (let [result (corpus/sweep property settings (constantly nil))
              failure (:failure result)]
          (t/is (= {:planned_trials 4, :passed_trials 0, :failed_trials 1, :unobserved_trials 3}
                   (dissoc result :failure)))
          (t/is (= [90 3 10] @inputs))
          (t/is (= 2 (:shrink_visits failure)))
          (t/is (every? #(= :invariant_failed (get-in failure [% :outcome]))
                        [:first_evidence :smallest_evidence]))
          (vreset! inputs [])
          (let [replay (corpus/replay property settings failure)]
            (t/is (:reproduced replay))
            (t/is (= [90 10] @inputs)))
          (vreset! inputs [])
          (t/is (thrown-with-msg? clojure.lang.ExceptionInfo #"did not regenerate"
                                  (corpus/replay property settings (assoc-in failure [:smallest :input_sha256] "bad"))))
          (t/is (empty? @inputs)))))))

(t/deftest a-true-predicate-without-native-observation-is-unobserved
  (let [property {:generators [(gen/return true)], :predicate identity}
        result (corpus/sweep property settings (constantly nil))]
    (t/is (zero? (:passed_trials result)))
    (t/is (= :unobserved (get-in result [:failure :first_evidence :outcome])))))

(t/deftest native-interval-projection-keeps-its-qualifier-literal
  (corpus/with-observers
    "defspec"
    (fn []
      (let [{:keys [outcome observations]}
            (corpus/observe (fn []
                              (let [integer (expression/project1 '(single-field-interval 3 "DAY" 2 0) {})
                                    text (expression/project1 '(single-field-interval "3" "DAY" 2 0) {})]
                                (and (instance? Interval integer) (= integer text))))
                            [])]
        (t/is (= :passed outcome))
        (t/is (= [:logical_query :logical_query] (mapv :kind observations)))))))
