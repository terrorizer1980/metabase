(ns metabase.mbql.normalize
  "Logic for taking any sort of weird MBQL query and normalizing it into a standardized, canonical form. You can think
  of this like taking any 'valid' MBQL query and rewriting it as-if it was written in perfect up-to-date MBQL in the
  latest version. There are two main things done here, done as three separate steps:

  #### NORMALIZING TOKENS

  Converting all identifiers to lower-case, lisp-case keywords. e.g. `{\"SOURCE_TABLE\" 10}` becomes `{:source-table
  10}`.

  #### CANONICALIZING THE QUERY

  Rewriting deprecated MBQL 95 syntax and other things that are still supported for backwards-compatibility in
  canonical MBQL 98 syntax. For example `{:breakout [:count 10]}` becomes `{:breakout [[:count [:field-id 10]]]}`.

  #### REMOVING EMPTY CLAUSES

  Removing empty clauses like `{:aggregation nil}` or `{:breakout []}`.

  Token normalization occurs first, followed by canonicalization, followed by removing empty clauses."
  (:require [clojure.walk :as walk]
            [medley.core :as m]
            ;; TODO - we should move `normalize-token` into this namespace.
            [metabase.query-processor.util :as qputil]
            [metabase.util :as u]))

(defn- mbql-clause? [x]
  (and (sequential? x)
       ((some-fn keyword? string?) (first x))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                NORMALIZE TOKENS                                                |
;;; +----------------------------------------------------------------------------------------------------------------+

(declare normalize-tokens)

(defn- normalize-expression-ref-tokens
  "For expression references (`[:expression \"my_expression\"]`) keep the arg as is but make sure it is a string."
  [_ expression-name]
  [:expression (if (keyword? expression-name)
                 (u/keyword->qualified-name expression-name)
                 expression-name)])

(defn- normalize-field-literal-tokens
  "Similarly, for Field literals, keep the arg as-is, but make sure it is a string."
  [_ field-name field-type]
  [:field-literal
   (if (keyword? field-name)
     (u/keyword->qualified-name field-name)
     field-name)
   (keyword field-type)])

(defn- normalize-datetime-field-tokens
  "Datetime fields look like `[:datetime-field <field> <unit>]` or `[:datetime-field <field> :as <unit>]`; normalize the
  unit, and `:as` (if present) tokens, and the Field."
  ([_ field unit]
   [:datetime-field (normalize-tokens field) (qputil/normalize-token unit)])
  ([_ field _ unit]
   [:datetime-field (normalize-tokens field) :as (qputil/normalize-token unit)]))

(defn- normalize-time-interval-tokens
  "`time-interval`'s `unit` should get normalized, and `amount` if it's not an integer."
  [_ field amount unit]
  [:time-interval
   (normalize-tokens field)
   (if (integer? amount)
     amount
     (qputil/normalize-token amount))
   (qputil/normalize-token unit)])

(defn- normalize-relative-datetime-tokens
  "Normalize a `relative-datetime` clause. `relative-datetime` comes in two flavors:

     [:relative-datetime :current]
     [:relative-datetime -10 :day] ; amount & unit"
  ([_ _]
   [:relative-datetime :current])
  ([_ amount unit]
   [:relative-datetime amount (qputil/normalize-token unit)]))

(def ^:private mbql-clause->special-token-normalization-fn
  "Special fns to handle token normalization for different MBQL clauses."
  {:expression        normalize-expression-ref-tokens
   :field-literal     normalize-field-literal-tokens
   :datetime-field    normalize-datetime-field-tokens
   :time-interval     normalize-time-interval-tokens
   :relative-datetime normalize-relative-datetime-tokens})

(defn- normalize-mbql-clause-tokens
  "MBQL clauses by default get just the clause name normalized (e.g. `[\"COUNT\" ...]` becomes `[:count ...]`) and the
  args are left as-is. If we need to do something special on top of that implement a fn in
  `mbql-clause->special-token-normalization-fn` above to handle the special normalization rules"
  [[clause-name & args]]
  (let [clause-name (qputil/normalize-token clause-name)]
    (if-let [f (mbql-clause->special-token-normalization-fn clause-name)]
      (apply f clause-name args)
      (vec (cons clause-name (map normalize-tokens args))))))


(defn- aggregation-subclause? [x]
  (or (when ((some-fn keyword? string?) x)
        (#{:avg :count :cum-count :distinct :stddev :sum :min :max} (qputil/normalize-token x)))
      (when (mbql-clause? x)
        (aggregation-subclause? (first x)))))

(defn- normalize-ag-clause-tokens
  "For old-style aggregations like `{:aggregation :count}` make sure we normalize the ag type (`:count`). Other wacky
  clauses like `{:aggregation [:count :count]}` need to be handled as well :("
  [ag-clause]
  (cond
    ;; something like {:aggregations :count}
    ((some-fn keyword? string?) ag-clause)
    (qputil/normalize-token ag-clause)

    ;; something wack like {:aggregations [:count [:sum 10]]} or {:aggregations [:count :count]}
    (when (mbql-clause? ag-clause)
      (aggregation-subclause? (second ag-clause)))
    (map normalize-ag-clause-tokens ag-clause)

    :else
    (normalize-tokens ag-clause)))

(defn- normalize-expressions-tokens
  "For expressions, we don't want to normalize the name of the expression; keep that as is, but make it a string;
   normalize the definitions as normal."
  [expressions-clause]
  (into {} (for [[expression-name definition] expressions-clause]
             [(if (keyword? expression-name)
                (u/keyword->qualified-name expression-name)
                expression-name)
              (normalize-tokens definition)])))

(defn- normalize-order-by-tokens
  "Normalize tokens in the order-by clause, which can have different syntax when using MBQL 95 or 98
  rules (`[<field> :asc]` vs `[:asc <field>]`, for example)."
  [clauses]
  (vec (for [subclause clauses]
         (if (mbql-clause? subclause)
           ;; MBQL 98 [direction field] style: normalize as normal
           (normalize-mbql-clause-tokens subclause)
           ;; otherwise it's MBQL 95 [field direction] style: flip the args and *then* normalize the clause. And then
           ;; flip it back to put it back the way we found it.
           (reverse (normalize-mbql-clause-tokens (reverse subclause)))))))

(def ^:private path->special-token-normalization-fn
  "Map of special functions that should be used to perform token normalization for a given path. For example, the
  `:expressions` key in an MBQL query should preserve the case of the expression names; this custom behavior is
  defined below."
  {:type   qputil/normalize-token
   ;; don't normalize native queries
   :native {:query identity}
   :query  {:aggregation normalize-ag-clause-tokens
            :expressions normalize-expressions-tokens
            :order-by    normalize-order-by-tokens}})

(defn- normalize-tokens
  "Recursively normalize a query and return the canonical form of that query."
  [x & [path]]
  ;; every time this function recurses it adds a new (normalized) key to key path, e.g. `path` will be
  ;; [:query :order-by] when we're in the MBQL order-by clause. If we need to handle these top-level clauses in special
  ;; ways add a function to `path->special-token-normalization-fn` above.
  (let [special-fn (when (seq path)
                     (get-in path->special-token-normalization-fn path))]
    (cond
      (fn? special-fn)
      (special-fn x)

      ;; Skip record types because this query is an `expanded` query, which is not going to play nice here. Hopefully we
      ;; can remove expanded queries entirely soon.
      (record? x)
      x

      ;; maps should just get the keys normalized and then recursively call normalize-tokens on the values.
      ;; Each recursive call appends to the keypath above so we can handle top-level clauses in a special way if needed
      (map? x)
      (into {} (for [[k v] x
                     :let  [k (qputil/normalize-token k)]]
                 [k (normalize-tokens v (conj (vec path) k))]))

      ;; MBQL clauses handled above because of special cases
      (mbql-clause? x)
      (normalize-mbql-clause-tokens x)

      ;; for non-mbql sequential collections (probably something like the subclauses of :order-by or something like
      ;; that) recurse on all the args. This doesn't append anything to path... (should it?)
      (sequential? x)
      (mapv normalize-tokens x)

      :else
      x)))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  CANONICALIZE                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- wrap-implicit-field-id
  "Wrap raw integer Field IDs (i.e., those in an implicit 'field' position) in a `:field-id` clause if they're not
  already. Done for MBQL 95 backwards-compatibility. e.g.:

    {:filter [:= 10 20]} ; -> {:filter [:= [:field-id 10] 20]}"
  [field]
  (if (integer? field)
    [:field-id field]
    field))

(defn- canonicalize-aggregation-subclause
  "Remove `:rows` type aggregation (long-since deprecated; simpliy means no aggregation) if present, and wrap
  `:field-ids` where appropriate."
  [[ag-type field, :as ag-subclause]]
  (cond
    (= ag-type :rows)
    nil

    field
    [ag-type (wrap-implicit-field-id field)]

    :else
    ag-subclause))

(defn- wrap-single-aggregations
  "Convert old MBQL 95 single-aggregations like `{:aggregation :count}` or `{:aggregation [:count]}` to MBQL 98
  multiple-aggregation syntax (e.g. `{:aggregation [[:count]]}`)."
  [aggregations]
  (cond
    ;; something like {:aggregations :count} -- MBQL 95 single aggregation
    (keyword? aggregations)
    [[aggregations]]

    ;; special-case: MBQL 98 multiple aggregations using unwrapped :count or :rows
    ;; e.g. {:aggregations [:count [:sum 10]]} or {:aggregations [:count :count]}
    (and (mbql-clause? aggregations)
         (aggregation-subclause? (second aggregations)))
    (reduce concat (map wrap-single-aggregations aggregations))

    ;; something like {:aggregations [:sum 10]} -- MBQL 95 single aggregation
    (mbql-clause? aggregations)
    [(vec aggregations)]

    :else
    (vec aggregations)))

(defn- canonicalize-aggregations
  "Canonicalize subclauses (see above) and make sure `:aggregation` is a sequence of clauses instead of a single
  clause."
  [aggregations]
  (->> (wrap-single-aggregations aggregations)
       (map canonicalize-aggregation-subclause)
       (filterv identity)))

(defn- canonicalize-filter [[filter-name & args, :as filter-subclause]]
  (cond
    ;; for `and` or `not` compound filters with only one subclase, just unnest the subclause
    (and (#{:and :or} filter-name)
         (= (count args) 1))
    (canonicalize-filter (first args))

    ;; for other `and`/`or`/`not` compound filters, recurse on the arg(s)
    (#{:and :or :not} filter-name)
    (vec (cons filter-name (map canonicalize-filter args)))

    ;; string filters should get the string implict filter options added if not specified explicitly
    (#{:starts-with :ends-with :contains :does-not-contain} filter-name)
    (let [[field arg {:keys [case-sensitive], :or {case-sensitive true}}] args]
      [filter-name (wrap-implicit-field-id field) arg {:case-sensitive case-sensitive}])

    ;; all the other filter types have an implict field ID for the first arg
    ;; (e.g. [:= 10 20] gets canonicalized to [:= [:field-id 10] 20]
    (#{:= :!= :< :<= :> :>= :is-null :not-null :between :inside :time-interval} filter-name)
    (apply vector filter-name (wrap-implicit-field-id (first args)) (rest args))))

(defn- canonicalize-order-by
  "Make sure order by clauses like `[:asc 10]` get `:field-id` added where appropriate, e.g. `[:asc [:field-id 10]]`"
  [order-by-clause]
  (vec (for [subclause order-by-clause
             :let      [[direction field-id] (if (#{:asc :desc :ascending :descending} (first subclause))
                                               ;; normal [<direction> <field>] clause
                                               subclause
                                               ;; MBQL 95 reversed [<field> <direction>] clause
                                               (reverse subclause))]]
         [(case direction
            :asc        :asc
            :desc       :desc
            ;; old MBQL 95 names
            :ascending  :asc
            :descending :desc)
          (wrap-implicit-field-id field-id)])))

(defn- canonicalize-top-level-mbql-clauses
  "Perform specific steps to canonicalize the various top-level clauses in an MBQL query."
  [mbql-query]
  (cond-> mbql-query
    (:aggregation mbql-query) (update :aggregation canonicalize-aggregations)
    (:breakout    mbql-query) (update :breakout    (partial mapv wrap-implicit-field-id))
    (:fields      mbql-query) (update :fields      (partial mapv wrap-implicit-field-id))
    (:filter      mbql-query) (update :filter      canonicalize-filter)
    (:order-by    mbql-query) (update :order-by    canonicalize-order-by)))


(def ^:private mbql-clause->canonicalization-fn
  {:fk->
   (fn [_ field-1 field-2]
     [:fk-> (wrap-implicit-field-id field-1) (wrap-implicit-field-id field-2)])

   :datetime-field
   (fn
     ([_ field unit]
      [:datetime-field (wrap-implicit-field-id field) unit])
     ([_ field _ unit]
      [:datetime-field (wrap-implicit-field-id field) unit]))

   :field-id
   (fn [_ id]
     ;; if someone is dumb and does something like [:field-id [:field-literal ...]] be nice and fix it for them.
     (if (mbql-clause? id)
       id
       [:field-id id]))})

(defn- canonicalize-mbql-clauses
  "Walk an `mbql-query` an canonicalize non-top-level clauses like `:fk->`."
  [mbql-query]
  (walk/prewalk
   (fn [clause]
     (if-not (mbql-clause? clause)
       clause
       (let [[clause-name & args] clause
             f                    (mbql-clause->canonicalization-fn clause-name)]
         (if f
           (apply f clause)
           clause))))
   mbql-query))

(defn- canonicalize
  "Canonicalize a query [MBQL query], rewriting the query as if you perfectly followed the recommended style guides for
  writing MBQL. Does things like removes unneeded and empty clauses, converts older MBQL '95 syntax to MBQL '98, etc."
  [outer-query]
  (cond-> outer-query
    (:query outer-query) (update :query (comp canonicalize-mbql-clauses canonicalize-top-level-mbql-clauses))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             REMOVING EMPTY CLAUSES                                             |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- non-empty-value?
  "Is this 'value' in a query map considered non-empty (e.g., should we refrain from removing that key entirely?) e.g.:

    {:aggregation nil} ; -> remove this, value is nil
    {:filter []}       ; -> remove this, also empty
    {:limit 100}       ; -> keep this"
  [x]
  (cond
    ;; a map is considered non-empty if it has some keys
    (map? x)
    (seq x)

    ;; a sequence is considered non-empty if it has some non-nil values
    (sequential? x)
    (and (seq x)
         (some some? x))

    ;; any other value is considered non-empty if it is not nil
    :else
    (some? x)))

(defn- remove-empty-clauses [query]
  (walk/postwalk
   (fn [x]
     (cond
       (record? x)
       x

       (map? x)
       (m/filter-vals non-empty-value? x)

       :else
       x))
   query))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                            PUTTING IT ALL TOGETHER                                             |
;;; +----------------------------------------------------------------------------------------------------------------+

(def ^{:arglists '([outer-query])} normalize
  "Normalize the tokens in an MBQL query (i.e., make them all `lisp-case` keywords), rewrite deprecated clauses as
  up-to-date MBQL 98, and remove empty clauses."
  (comp remove-empty-clauses canonicalize normalize-tokens))
