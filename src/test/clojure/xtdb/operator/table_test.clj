(ns xtdb.operator.table-test
  (:require [clojure.spec.alpha :as s]
            [clojure.test :as t]
            [xtdb.expression :as expr]
            [xtdb.logical-plan :as lp]
            [xtdb.test-util :as tu]
            [xtdb.time :as time])
  (:import (java.nio BufferUnderflowException ByteBuffer)
           (java.nio.charset StandardCharsets)
           (java.time Duration LocalDate LocalDateTime LocalTime ZonedDateTime)
           (java.util.function Consumer)
           (xtdb.api ICursor)
           (xtdb.arrow FixedSizeBinaryVector Relation RelationReader UuidVector Vector VectorWriterKt)))

(t/use-fixtures :each tu/with-allocator tu/with-node)

(t/deftest test-table
  (t/is (= {:res [{:a 12, :b "foo" :c 1.2, :e true, :f (Duration/ofHours 1)}
                  {:a 100, :b "bar", :c 3.14, :d (time/->zdt #inst "2020"), :e 10, :f (Duration/ofMinutes 1)}]
            :types {'a #xt/type :i64, 'b #xt/type :utf8, 'c #xt/type :f64,
                    'd #xt/type [:? :instant]
                    'e #xt/type #{:bool :i64}
                    'f #xt/type [:duration :micro]}}
           (-> (tu/query-ra '[:table {:output-cols [a b c d e f], :param ?table}]
                            {:args {:table [{:a 12, :b "foo" :c 1.2 :d nil :e true :f (Duration/ofHours 1)}
                                            {:a 100, :b "bar", :c 3.14, :d #inst "2020", :e 10, :f (Duration/ofMinutes 1)}]}
                             :with-types? true}))))

  (t/is (= {:res [{:a 12, :b "foo", :c 1.2, :e true}
                  {:a 100, :b "bar", :c 3.14, :d (time/->zdt #inst "2020"), :e 10}]
            :types '{a #xt/type :i64, b #xt/type :utf8, c #xt/type :f64,
                     d #xt/type [:? :instant]
                     e #xt/type #{:i64 :bool}}}
           (-> (tu/query-ra '[:table {:rows [{:a 12, :b "foo", :c 1.2, :d nil, :e true}
                                             {:a 100, :b "bar", :c 3.14, :d #inst "2020", :e 10}]}]
                            {:with-types? true})))
        "inline table")

  (t/is (= {:res [], :types {}}
           (-> (tu/query-ra '[:table {:param ?table}]
                            {:args {:table []}
                             :with-types? true})))
        "empty")

  (t/is (= {:res [{:a 12, :b "foo"}, {:a 100}]
            :types '{a #xt/type :i64, b #xt/type [:? :utf8]}}
           (-> (tu/query-ra '[:table {:param ?table}]
                            {:args {:table [{:a 12, :b "foo"}
                                            {:a 100}]}
                             :with-types? true})))
        "differing columns")

  (t/is (= {:res [{:a 12}]
            :types '{a #xt/type :i64}}
           (-> (tu/query-ra '[:table {:output-cols [a], :param ?table}]
                            {:args {:table [{:a 12, :b "foo"}]}
                             :with-types? true})))
        "restricts to provided col-names")

  (t/is (= [{} {} {}]
           (tu/query-ra [:table {:rows [{} {} {}]}]
                        {}))
        "table with no cols")

  (t/is (= [{:a #{1 3 2}}]
           (tu/query-ra [:table {:rows [{:a #{1 2 3}}]}]
                        {}))
        "table with sets"))

(t/deftest test-table-handles-exprs
  (t/is (= [{:a 3, :b false}
            {:b [24 24]}
            {:a 3, :b 4}]
           (tu/query-ra '[:table {:rows [{:a (+ 1 2), :b (> 3 4)}
                                         {:a nil, :b [24 (* 3 8)]}
                                         {:a 3, :b 4}]}]
                        {})))

  (t/is (= [{:a 1, :b 2}, {:a 3, :b 4}]
           (tu/query-ra '[:table {:rows [{:a ?p1, :b ?p2}
                                         {:a ?p3, :b ?p4}]}]
                        {:args {:p1 1, :p2 2, :p3 3, :p4 4}})))

  (t/is (= [{:a {:baz 1}, :b 2}]
           (tu/query-ra '[:table {:rows [{:a {:baz ?p1} :b ?p2}]}]
                        {:args {:p1 1, :p2 2, :p3 3, :p4 4}}))
        "nested param")

  (t/is (= [{:foo :bar, :baz {:nested-foo :bar}}]
           (tu/query-ra '[:table {:rows [{:foo :bar, :baz {:nested_foo ?nested_param}}]}]
                        {:args {:nested_param :bar}}))
        "nested param with need for normalisation"))

(t/deftest test-table-handles-symbols
  (t/is (= '[{:x50 true}]
           (tu/query-ra '[:sort {:limit 1}
                          [:union-all {}
                           [:project
                            {:projections [{x50 true}]}
                            [:select {:predicate (== ?x53 x48)} [:table {:rows [{x48 "AIR"} {x48 "AIR REG"}]}]]]
                           [:table {:rows [{x50 false}]}]]]
                        {:args {:x53 "AIR"}})))

  (t/is (= '[{:x50 true}]
           (tu/query-ra '[:sort {:limit 1}
                          [:union-all {}
                           [:project
                            {:projections [{x50 true}]}
                            [:select {:predicate (== ?x53 x48)} [:table {:rows [{x48 "AIR"} {x48 "AIR REG"}]}]]]
                           [:table {:rows [{x50 false}]}]]]
                        {:args {:x53 "AIR REG"}})))

  (t/is (= '[{:x50 false}]
           (tu/query-ra '[:sort {:limit 1}
                          [:union-all {}
                           [:project
                            {:projections [{x50 true}]}
                            [:select {:predicate (== ?x53 x48)} [:table {:rows [{x48 "AIR"} {x48 "AIR REG"}]}]]]
                           [:table {:rows [{x50 false}]}]]]
                        {:args {:x53 "RAIL"}}))))

(t/deftest test-incorrect-relation-params
  (t/is
   (thrown-with-msg? RuntimeException #"Table param must be of type struct list"
                     (tu/query-ra '[:table {:param ?a}]
                                  {:args {:a 1}}))
   "param not list type")
  (t/is
   (thrown-with-msg? RuntimeException #"Table param must be of type struct list"
                     (tu/query-ra '[:table {:param ?a}]
                                  {:args {:a [1 "foo" :foo]}}))
   "param not struct list type"))

(t/deftest do-not-leak-memory-on-expression-errors
  (t/is
   (thrown-with-msg? RuntimeException #"division by zero"
                     (tu/query-ra '[:table {:output-cols [x9], :rows [{x9 -54} {x9 (/ 35 0)}]}]
                                  {:node tu/*node*}))))

(t/deftest test-absent-columns
  (t/is (= '{:res [{:x5 1} {:x6 -77}],
             :types {x5 #xt/type [:? :i64], x6 #xt/type [:? :i64]}}
           (tu/query-ra
            '[:table {:rows [{:x5 1} {:x6 -77}]}]
            {:with-types? true})))

  (t/is (= '{:res [{:x5 1} {:x6 -77}],
             :types {x5 #xt/type [:? :i64], x6 #xt/type [:? :i64]}}
           (tu/query-ra '[:table {:param ?table}]
                        {:args {:table [{:x5 1} {:x6 -77}]}
                         :with-types? true}))
        "differing columns")

  (t/is (= {:res [{:a 12.4, :b 10} {:b 15} {:a 100, :b 83} {:a 83.0, :b 100}],
            :types '{a #xt/type #{:f64 :null :i64}, b #xt/type :i64}}

           (tu/query-ra '[:table {:param ?table}]
                        {:args {:table [{:a 12.4, :b 10}, {:a nil, :b 15}, {:a 100, :b 83}, {:a 83.0, :b 100}]}
                         :with-types? true}))
        "actual nils"))

(t/deftest test-single-col
  (t/is (= '{:res [{:x5 1} {:x5 2} {:x5 3}],
             :types {x5 #xt/type :i64}}
           (tu/query-ra '[:table {:column {x5 [1 2 3]}}]
                        {:with-types? true})))

  (t/is (= '{:res [{:x5 1} {:x5 2.0} {:x5 3}],
             :types {x5 #xt/type #{:i64 :f64}}}
           (tu/query-ra '[:table {:column {x5 [1 2.0 3]}}]
                        {:with-types? true})))

  (t/is (= {:res [{:unnest-param 12.4} {} {:unnest-param 100} {:unnest-param 83.0}],
            :types '{unnest-param #xt/type #{:f64 :i64 :null}}}

           (tu/query-ra '[:table {:column {unnest-param ?coll}}]
                        {:args {:coll [12.4, nil, 100, 83.0]}
                         :with-types? true})))

  (t/is (= {:res [], :types '{b #xt/type :null}}
           (tu/query-ra '[:table {:column {b nil}}]
                        {:with-types? true}))
        "nil value - #4075"))

(t/deftest test-table-with-map-params
  (t/is (= [{:a 4.2} {:b "1", :c 2, :a 0} {:b 2, :a 1}]
           (tu/query-ra '[:table {:rows [{:a 4.2} ?record {:a 1, :b 2}]}]
                        {:args {:record {:a 0, :b "1", :c 2}}}))))

(t/deftest values-row-parameters-and-computed-cells-preserve-column-types
  (t/is (= '{:res [{:numeric-value 12, :label "record"}
                   {:numeric-value 8, :label "é", :flag false}
                   {:flag true}]
             :types {numeric_value #xt/type [:? :i64]
                     label #xt/type [:? :utf8]
                     flag #xt/type [:? :bool]}}
           (tu/query-ra '[:table {:rows [?record
                                        {:numeric_value (if false (/ 1 0) (+ ?n 1)),
                                         :label ?label,
                                         :flag ?flag}
                                        {:flag true}]}]
                        {:args {:record {:numeric-value 12, :label "record"}
                                :n 7, :label "é", :flag false}
                         :with-types? true}))))

(t/deftest values-and-records-preserve-primitive-list-child-types
  (t/is (= '{:res [{:xs [1 nil -2]} {:xs [5 nil 6]} {:xs [nil 9]} {} {}]
             :types {xs #xt/type [:? [:list [:? :i64]]]}}
           (tu/query-ra '[:table {:rows [{:xs ?values}
                                        {:xs [?n nil (+ ?n 1)]}
                                        ?record
                                        {:xs nil}
                                        {}]}]
                        {:args {:values [1 nil -2], :n 5, :record {:xs [nil 9]}}
                         :with-types? true}))))

(t/deftest values-and-records-preserve-bound-uuid-and-date-day-types
  (let [id #uuid "fedcba98-7654-3210-8123-456789abcdef"
        first-day (LocalDate/ofEpochDay Integer/MIN_VALUE)
        last-day (LocalDate/ofEpochDay Integer/MAX_VALUE)]
    (t/is (= {:res [{:id id, :day first-day}
                    {:id id, :day last-day}
                    {:id 7}
                    {}]
              :types '{id #xt/type #{:uuid :i64 :null}, day #xt/type [:? [:date :day]]}}
             (tu/query-ra '[:table {:rows [{:id ?id, :day ?day}
                                          ?record
                                          {:id 7, :day nil}
                                          {}]}]
                          {:args {:id id, :day first-day, :record {:id id, :day last-day}}
                           :with-types? true})))))

(t/deftest values-uuid-cells-preserve-checked-noncanonical-widths
  (with-open [^Vector out (tu/open-vec "u" #xt/type :uuid [])
              short-source (UuidVector. (FixedSizeBinaryVector. tu/*allocator* "short" false 8))
              wide-source (UuidVector. (FixedSizeBinaryVector. tu/*allocator* "wide" false 24))]
    (.writeBytes short-source (ByteBuffer/wrap (byte-array (range 8))))
    (.writeBytes wide-source (ByteBuffer/wrap (byte-array (range 24))))
    (t/is (thrown? BufferUnderflowException (VectorWriterKt/writeScalarCell out short-source 0)))
    (t/is (zero? (.getValueCount out)))
    (VectorWriterKt/writeScalarCell out wide-source 0)
    (t/is (= #uuid "00010203-0405-0607-0809-0a0b0c0d0e0f" (.getObject out 0)))
    (t/is (= (ByteBuffer/wrap (byte-array (range 16))) (.getBytes out 0)))))

(t/deftest computed-values-preserve-uuid-and-date-day-layouts
  (let [id #uuid "fedcba98-7654-3210-8123-456789abcdef"
        first-day (LocalDate/ofEpochDay Integer/MIN_VALUE)
        last-day (LocalDate/ofEpochDay Integer/MAX_VALUE)]
    (t/is (= {:res [{:id id, :day first-day, :mixed-id id, :mixed-day last-day}
                    {:day last-day, :mixed-id 7, :mixed-day "other"}]
              :types '{id #xt/type [:? :uuid], day #xt/type [:date :day],
                       mixed_id #xt/type #{:uuid :i64}, mixed_day #xt/type #{[:date :day] :utf8}}}
             (tu/query-ra '[:table {:rows [{:id (cast ?id #xt/type :uuid),
                                           :day (cast ?first_day #xt/type [:date :day]),
                                           :mixed_id (cast ?id #xt/type :uuid),
                                           :mixed_day (cast ?last_day #xt/type [:date :day])}
                                          {:id (cast ?absent #xt/type :uuid),
                                           :day (cast ?last_day #xt/type [:date :day]),
                                           :mixed_id 7, :mixed_day "other"}]}]
                          {:args {:id (str id), :absent nil,
                                  :first_day (str first-day), :last_day (str last-day)}
                           :with-types? true})))
    (t/is (thrown-with-msg? RuntimeException #"Invalid UUID string"
                            (tu/query-ra '[:table {:rows [{:id (cast ?id #xt/type :uuid)}]}]
                                         {:args {:id "bad"}})))))

(t/deftest values-and-records-preserve-bound-temporal-layouts
  (let [ts (LocalDateTime/parse "1969-12-31T23:59:59.999999")
        at (ZonedDateTime/parse "2025-11-02T01:30:00-05:00[America/New_York]")
        duration (Duration/ofNanos -1000)
        row {:ts ts, :at at, :duration duration, :clock LocalTime/MAX}]
    (t/is (= {:res [row (assoc row :clock LocalTime/MIDNIGHT) {:at 7} {}]
              :types '{ts #xt/type [:? [:timestamp-local :micro]],
                       at #xt/type #{[:timestamp-tz :micro "America/New_York"] :i64 :null},
                       duration #xt/type [:? [:duration :micro]], clock #xt/type [:? [:time-local :nano]]}}
             (tu/query-ra '[:table {:rows [{:ts ?ts, :at ?at, :duration ?duration, :clock ?clock}
                                          ?record
                                          {:at 7}
                                          {}]}]
                          {:args (assoc row :record (assoc row :clock LocalTime/MIDNIGHT))
                           :with-types? true})))))

(t/deftest computed-values-preserve-microsecond-temporal-types
  (let [^LocalDateTime local (LocalDateTime/parse "1970-01-01T00:00:00")
        ^ZonedDateTime zoned (ZonedDateTime/parse "2025-11-02T01:30:00-05:00[America/New_York]")
        step (Duration/ofNanos -1000)]
    (t/is (= {:res [{:ts (.minusNanos local 1000), :at (.minusNanos zoned 1000),
                     :elapsed (Duration/ofNanos -2000), :mixed (Duration/ofNanos -2000)}
                    {:mixed 7}]
              :types '{ts #xt/type [:? [:timestamp-local :micro]],
                       at #xt/type [:? [:timestamp-tz :micro "America/New_York"]],
                       elapsed #xt/type [:? [:duration :micro]],
                       mixed #xt/type #{[:duration :micro] :i64}}}
             (tu/query-ra '[:table {:rows [{:ts (+ ?local ?step), :at (+ ?zoned ?step),
                                           :elapsed (+ ?step ?step), :mixed (+ ?step ?step)}
                                          {:ts (cast ?absent #xt/type [:timestamp-local :micro]),
                                           :mixed 7}]}]
                          {:args {:local local, :zoned zoned, :step step, :absent nil}
                           :with-types? true})))))

(t/deftest values-text-cells-preserve-java-utf8-replacement
  (let [^bytes malformed (byte-array (map unchecked-byte [0xed 0xa0 0x80 0x61 0xf0 0x9f]))
        replaced (String. malformed StandardCharsets/UTF_8)]
    (with-open [args (Relation. tu/*allocator* ^java.util.Map {"?label" #xt/type :utf8})]
      (.writeBytes (.vectorFor args "?label") malformed)
      (.endRow args)
      (let [{:keys [->cursor vec-types]}
            (lp/emit-expr (s/conform ::lp/logical-plan
                                     '[:table {:rows [{:bound ?label,
                                                       :computed (concat ?label "é"),
                                                       :encoded (upper ?label)}]}])
                          {:param-types (expr/->param-types args)})]
        (t/is (= '{bound #xt/type :utf8, computed #xt/type :utf8, encoded #xt/type :utf8}
                 vec-types))
        (with-open [^ICursor cursor (->cursor {:allocator tu/*allocator*, :args args, :schema {}})]
          (t/is (.tryAdvance cursor
                             (reify Consumer
                               (accept [_ out]
                                 (let [^RelationReader out out]
                                   (doseq [[col ^String expected] {"bound" replaced,
                                                                   "computed" (str replaced "é"),
                                                                   "encoded" (.toUpperCase replaced)}]
                                     (t/is (= (seq (.getBytes expected StandardCharsets/UTF_8))
                                              (-> (.vectorFor out col) (.getBytes 0) expr/resolve-bytes seq))))))))))))))

(t/deftest values-known-utf8-writers-append-after-other-rows
  (t/is (= '{:res [{:text "😀é"} {:text "É😀"} {:text "🎈"}]
             :types {text #xt/type :utf8}}
           (tu/query-ra '[:table {:rows [{:text "😀é"}
                                        {:text (upper ?label)}
                                        {:text ?rest}]}]
                        {:args {:label "é😀", :rest "🎈"}, :with-types? true}))))

(t/deftest positioned-record-projections-and-patch-preserve-null-ancestors
  (t/is (= [{:patched {:nested {:leaf "left"}, :kept 1}
             :direct {:nested {:leaf "left"}, :kept 1}
             :selected "é",
             :row {:chosen {:leaf "é"}}}
            {:patched {:nested {:leaf "right"}, :kept 2}
             :direct {:nested {:leaf "stale"}, :kept 2}, :row {}}
            {:patched {:nested {:leaf "keep"}, :kept 3}
             :direct {:nested {:leaf "keep"}, :kept 3}, :row {}}]
           (tu/query-ra
            '[:project {:projections [{patched (_patch l r)}
                                      {direct l}
                                      {selected (.. p inner leaf)}
                                      {row {:chosen (. p inner)}}]}
              [:table {:rows [{:l {:nested {:leaf "left"}, :kept 1}
                               :r {:nested nil}
                               :p {:inner {:leaf "é"}}}
                              {:l {:nested {:leaf "stale"}, :kept 2}
                               :r {:nested {:leaf "right"}}
                               :p nil}
                              {:l {:nested {:leaf "keep"}, :kept 3}
                               :r {:nested nil}
                               :p {:inner nil}}]}]]
            {}))))

(t/deftest positioned-patch-preserves-date-day-layout-and-absent-fields
  (let [first-day (LocalDate/ofEpochDay Integer/MIN_VALUE)
        last-day (LocalDate/ofEpochDay Integer/MAX_VALUE)
        result (tu/query-ra
                '[:project {:projections [{patched (_patch l r)}]}
                  [:table {:rows [{:l {:day ?first_day, :kept 1}, :r {:day nil, :extra nil}}
                                  {:l {:day ?first_day, :kept 2}, :r {:day ?last_day, :extra ?first_day}}
                                  {:l {:day nil, :kept 3}, :r {:day nil, :extra nil}}]}]]
                {:args {:first_day first-day, :last_day last-day}, :with-types? true})]
    (t/is (= {:res [{:patched {:day first-day, :kept 1}}
                    {:patched {:day last-day, :kept 2, :extra first-day}}
                    {:patched {:kept 3}}]
              :types '{patched #xt/type [:struct {"day" [:? [:date :day]], "kept" :i64,
                                                 "extra" [:? [:date :day]]}]}}
             result))))

(t/deftest positioned-interval-projection-keeps-null-and-signed-components
  (t/is (= [{:copied #xt/interval "P12M"} {} {:copied #xt/interval "P-4M"}]
           (tu/query-ra
            '[:project {:projections [{copied tick}]}
              [:table {:rows [{:tick #xt/interval "P12M"} {:tick nil} {:tick #xt/interval "P-4M"}]}]]
            {}))))

(t/deftest test-table-stats
  (t/testing "row table stats - should return stats/row-count"
    (t/is (= {:row-count 4}
             (:stats
              (lp/emit-expr (s/conform ::lp/logical-plan
                                       '[:table {:rows [{:foo 1 :bar "woo"}
                                                        {:foo 2 :bar "yay"}
                                                        {:foo 3 :bar "yipee"}
                                                        {:foo 4 :bar "huzzah"}]}])
                            {})))))

  ;; Other types of table do NOT currently return stats
  (t/testing "col table stats"
    (t/is (nil? (:stats (lp/emit-expr (s/conform ::lp/logical-plan
                                                 '[:table {:column {foo [1 2 3 4]}}])
                                      {}))))))
