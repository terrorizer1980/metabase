(ns metabase.mbql.util
  "Utilitiy functions for working with MBQL queries."
  (:require [clojure
             [string :as str]
             [walk :as walk]]
            [metabase.util :as u]
            [metabase.util.schema :as su]
            [schema.core :as s]))

(s/defn normalize-token :- s/Keyword
  "Convert a string or keyword in various cases (`lisp-case`, `snake_case`, or `SCREAMING_SNAKE_CASE`) to a lisp-cased
  keyword."
  [token :- su/KeywordOrString]
  (-> (u/keyword->qualified-name token)
      str/lower-case
      (str/replace #"_" "-")
      keyword))

;; TODO - perhaps these definitions that support pre-normalized clauses should be moved into `normalize` and we should
;; just have simple definitions here instead
(defn mbql-clause?
  "True if `x` is an MBQL clause (a sequence with a token as its first arg). (Since this is used by the code in
  `normalize` this handles pre-normalized clauses as well.)"
  [x]
  (and (sequential? x)
       ((some-fn keyword? string?) (first x))))

(defn is-clause?
  "If `x` an MBQL clause, and an instance of clauses defined by keyword(s) `k-or-ks`?

    (is-clause? :count [:count 10])        ; -> true
    (is-clause? #{:+ :- :* :/} [:+ 10 20]) ; -> true

  (Since this is used by the code in `normalize` this handles pre-normalized clauses as well.)"
  [k-or-ks x]
  (and
   (mbql-clause? x)
   (let [clause-name (normalize-token (first x))]
     (if (coll? k-or-ks)
       ((set k-or-ks) clause-name)
       (= k-or-ks clause-name)))))

(defn clause-instances
  "Return a sequence of all the instances of clause(s) in `x`. Like `is-clause?`, you can either look for instances of a
  single clause by passing a single keyword or for instances of multiple clauses by passing a set of keywords.

    ;; look for :field-id clauses
    (clause-instances :field-id {:query {:filter [:= [:field-id 10] 20]}})
    ;;-> [[:field-id 10]]

    ;; look for :+ or :- clauses
    (clause-instances #{:+ :-} ...)"
  {:style/indent 1}
  [k-or-ks x]
  (let [instances (transient [])]
    (walk/postwalk
     (fn [clause]
       (u/prog1 clause
         (when (is-clause? k-or-ks clause)
           (conj! instances clause))))
     x)
    (persistent! instances)))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                       Functions for manipulating queries                                       |
;;; +----------------------------------------------------------------------------------------------------------------+

;; TODO - we should probably validate input and output of these at some point. But that would require rewriting tests
;; with invalid queries so we can do that later

(defn add-filter-clause
  "Add an additional filter clause to an `outer-query`."
  [outer-query new-clause]
  (update-in outer-query [:query :filter] (fn [existing-clause]
                                            (cond
                                              ;; if top-level clause is `:and` then just add the new clause at the end
                                              (is-clause? :and existing-clause)
                                              (conj existing-clause new-clause)

                                              ;; otherwise if we have an existing clause join to new one with an `:and`
                                              existing-clause
                                              [:and existing-clause new-clause]

                                              ;; if we don't have existing clause then new clause is the new top-level
                                              :else
                                              new-clause))))
