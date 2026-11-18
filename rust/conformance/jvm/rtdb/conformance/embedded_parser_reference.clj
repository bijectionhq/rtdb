(ns rtdb.conformance.embedded-parser-reference
  (:require [rtdb.conformance.embedded-parser :as observer])
  (:import (org.antlr.v4.runtime CharStreams CommonTokenStream InputMismatchException)
           (org.antlr.v4.runtime.atn ParserATNSimulator)
           (xtdb.antlr Sql SqlLexer)
           (xtdb.api.error Incorrect)
           (xtdb.query SqlParser)))

(set! *warn-on-reflection* true)

(defn- mismatch-cause [^InputMismatchException mismatch ^String follow-up]
  (let [recognizer (.getRecognizer mismatch)]
    {:class (observer/throwable-class mismatch)
     :recognizer_class (some-> recognizer class .getName)
     :offending_state (.getOffendingState mismatch)
     :shared_decision_array
     (if (instance? Sql recognizer)
       (let [^Sql parser recognizer
             other (Sql. (CommonTokenStream. (SqlLexer. (CharStreams/fromString follow-up))))
             ^ParserATNSimulator interpreter (.getInterpreter parser)
             ^ParserATNSimulator other-interpreter (.getInterpreter other)
             decisions (.-decisionToDFA interpreter)]
         (and (not (identical? parser other))
              (not (identical? interpreter other-interpreter))
              (some? decisions)
              (identical? decisions (.-decisionToDFA other-interpreter))))
       false)}))

(defn- parse-statement [^String sql ^String follow-up]
  (try
    {:outcome (observer/returned (some-> (SqlParser/parseStatementAst sql) class .getName))}
    (catch Incorrect error
      (let [category (:cognitect.anomalies/category (ex-data error))
            code (:xtdb.error/code (ex-data error))
            cause (ex-cause error)
            signature {:cause (if (instance? InputMismatchException cause)
                                (mismatch-cause cause follow-up)
                                (observer/cause-class cause))}]
        {:outcome (observer/failure "parse_statement_ast" (when (keyword? category) (name category))
                                    (when (keyword? code) (subs (str code) 1)) signature error)
         :message (ex-message error)}))))

(defn- observe [{:keys [sql]}]
  (let [{:keys [statement_mismatch unwanted_token follow_up]} sql
        mismatch (parse-statement statement_mismatch follow_up)]
    {:observations [(assoc (select-keys mismatch [:outcome]) :id "statement_mismatch" :executed true)]
     :supplemental {:statement_mismatch (dissoc mismatch :outcome)
                    :unwanted_token (parse-statement unwanted_token follow_up)
                    :follow_up (parse-statement follow_up follow_up)}}))

(defn -main [fixture-path output-path]
  (observer/observe-fixture! "reference" [SqlParser Sql InputMismatchException] nil observe fixture-path output-path))
