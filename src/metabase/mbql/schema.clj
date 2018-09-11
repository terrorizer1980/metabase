(ns metabase.mbql.schema
  "Schema for validating a *normalized* MBQL query. This is also the definitive grammar for MBQL, wow."
  (:refer-clojure :exclude [count distinct min max + - / * and or not = < > <= >=])
  (:require [clojure.core :as core]
            [metabase.mbql.schema.helpers :refer [defclause is-clause? one-of]]
            [metabase.util
             [date :as du]
             [schema :as su]]
            [schema.core :as s]))

;;; ------------------------------------------------- Datetime Stuff -------------------------------------------------

(def ^:private DatetimeFieldUnit
  (s/named
   (apply s/enum #{:default :minute :minute-of-hour :hour :hour-of-day :day :day-of-week :day-of-month :day-of-year
                   :week :week-of-year :month :month-of-year :quarter :quarter-of-year :year})
   "datetime-unit"))

(def ^:private RelativeDatetimeUnit
  (s/named
   (apply s/enum #{:minute :hour :day :week :month :quarter :year})
   "relative-datetime-unit"))

(def ^:private LiteralDatetimeString
  "Schema for an MBQL datetime string literal, in ISO-8601 format."
  (s/constrained su/NonBlankString du/date-string? "datetime-literal"))

(defclause relative-datetime
  n    (s/cond-pre (s/eq :current) s/Int)
  unit (optional RelativeDatetimeUnit))

(def ^:private DatetimeLiteral
  "Schema for valid absoulute datetime literals."
  (s/cond-pre
   LiteralDatetimeString
   java.sql.Date
   java.util.Date))


;;; ----------------------------------------------------- Fields -----------------------------------------------------

(defclause field-id, id su/IntGreaterThanZero)

(defclause field-literal, field-name su/NonBlankString, field-type su/FieldType)

(defclause expression, expression-name su/NonBlankString)

(defclause fk->
  source-field (one-of field-id field-literal)
  dest-field   (one-of field-id field-literal))

(defclause datetime-field
  field (one-of field-id field-literal fk-> expression)
  unit  DatetimeFieldUnit)

(def ^:private Field
  (one-of field-id field-literal fk-> datetime-field expression))

;; aggregate field reference
(defclause aggregation, aggregation-clause-index s/Int)


;;; ------------------------------------------------------- Ag -------------------------------------------------------

(defclause count,     field (optional Field))
(defclause avg,       field Field)
(defclause cum-count, field (optional Field))
(defclause cum-sum,   field Field)
(defclause distinct,  field Field)
(defclause stddev,    field Field)
(defclause sum,       field Field)
(defclause min,       field Field)
(defclause max,       field Field)

;; Metrics are just 'macros' (placeholders for other aggregations) that get expanded to some other aggregation in the
;; expand-macros middleware
(defclause metric, metric-id su/IntGreaterThanZero)

;; the following are definitions for expression aggregations, e.g. [:+ [:sum [:field-id 10]] [:sum [:field-id 20]]]

(declare UnnamedAggregation)

(def ^:private ExpressionAggregationArg
  (s/if number?
    s/Num
    (s/recursive #'UnnamedAggregation)))

(defclause [ag:+   +],  x ExpressionAggregationArg, y ExpressionAggregationArg, more (rest ExpressionAggregationArg))
(defclause [ag:-   -],  x ExpressionAggregationArg, y ExpressionAggregationArg, more (rest ExpressionAggregationArg))
(defclause [ag:*   *],  x ExpressionAggregationArg, y ExpressionAggregationArg, more (rest ExpressionAggregationArg))
(defclause [ag:div /],  x ExpressionAggregationArg, y ExpressionAggregationArg, more (rest ExpressionAggregationArg))
;; ag:/ isn't a valid token

(def ^:private UnnamedAggregation
  (one-of count avg cum-count cum-sum distinct stddev sum min max ag:+ ag:- ag:* ag:div metric))

;; any sort of aggregation can be wrapped in a `[:named <ag> <custom-name>]` clause, but you cannot wrap a `:named` in
;; a `:named`

(defclause named, aggregation UnnamedAggregation, aggregation-name su/NonBlankString)

(def ^:private Aggregation
  (s/if (partial is-clause? :named)
    named
    UnnamedAggregation))

;;; -------------------------------------------------- Expressions ---------------------------------------------------

(declare ExpressionDef)

(def ^:private ExpressionArg
  (s/conditional
   number?
   s/Num

   (every-pred vector? #{:+ :- :/ :*})
   (s/recursive #'ExpressionDef)

   :else
   Field))

(defclause +, x ExpressionArg, y ExpressionArg)
(defclause -, x ExpressionArg, y ExpressionArg)
(defclause /, x ExpressionArg, y ExpressionArg)
(defclause *, x ExpressionArg, y ExpressionArg)

(def ^:private ExpressionDef
  (one-of + - / *))


;;; ----------------------------------------------------- Filter -----------------------------------------------------

(declare Filter)

(defclause and
  first-clause  (s/recursive #'Filter)
  second-clause (s/recursive #'Filter)
  other-clauses (rest (s/recursive #'Filter)))

(defclause or
  first-clause  (s/recursive #'Filter)
  second-clause (s/recursive #'Filter)
  other-clauses (rest (s/recursive #'Filter)))

(defclause not, clause (s/recursive #'Filter))

(def ^:private FieldOrRelativeDatetime
  (s/if (partial is-clause? :relative-datetime)
   relative-datetime
   Field))

(def ^:private EqualityComparible
  "Schema for things things that make sense in a `=` or `!=` filter, i.e. things that can be compared for equality."
  (s/maybe
   (s/cond-pre
    s/Bool
    s/Num
    s/Str
    DatetimeLiteral
    FieldOrRelativeDatetime)))

(def ^:private OrderComparible
  "Schema for things that make sense in a filter like `>` or `<`, i.e. things that can be sorted."
  (s/cond-pre
   s/Num
   DatetimeLiteral
   FieldOrRelativeDatetime))

(defclause =,  field Field, value-or-field EqualityComparible, more-values-or-fields (rest EqualityComparible))
(defclause !=, field Field, value-or-field EqualityComparible, more-values-or-fields (rest EqualityComparible))

(defclause <,  field Field, value-or-field OrderComparible)
(defclause >,  field Field, value-or-field OrderComparible)
(defclause <=, field Field, value-or-field OrderComparible)
(defclause >=, field Field, value-or-field OrderComparible)

(defclause between field Field, min OrderComparible, max OrderComparible)

(defclause inside
  lat-field Field
  lon-field Field
  lat-max   OrderComparible
  lon-min   OrderComparible
  lat-min   OrderComparible
  lon-max   OrderComparible)

(defclause is-null,  field Field)
(defclause not-null, field Field)

(def ^:private StringFilterOptions
  {:case-sensitive s/Bool})

(def ^:private StringOrField
  (s/cond-pre
   s/Str
   Field))

(defclause starts-with,      field Field, string-or-field StringOrField, options StringFilterOptions)
(defclause ends-with,        field Field, string-or-field StringOrField, options StringFilterOptions)
(defclause contains,         field Field, string-or-field StringOrField, options StringFilterOptions)
(defclause does-not-contain, field Field, string-or-field StringOrField, options StringFilterOptions)

(defclause time-interval
  field Field
  n     (s/cond-pre
         s/Int
         (s/enum :current :last :next))
  unit  RelativeDatetimeUnit)

(def ^:private Filter
  (one-of and or not = != < > <= >= between inside is-null not-null starts-with ends-with contains does-not-contain
          time-interval))


;;; ----------------------------------------------------- Query ------------------------------------------------------

(declare MBQLQuery)

;; TODO - schemas for template tags and dimensions live in `metabase.query-processor.middleware.parameters.sql`. Move
;; them here when we get the chance.

(def ^:private TemplateTag
  s/Any) ; s/Any for now until we move over the stuff from the parameters middleware

(def NativeQuery
  "Schema for a valid, normalized native [inner] query."
  {:native                         s/Any
   (s/optional-key :template-tags) {s/Keyword TemplateTag}}) ; TODO - I think template tag keys should be strings


(def ^:private SourceQuery
  "Schema for a valid value for a `:source-query` clause."
  (s/if :native
    NativeQuery
    (s/recursive #'MBQLQuery)))

(def MBQLQuery
  "Schema for a valid, normalized MBQL [inner] query."
  (s/constrained
   {(s/optional-key :source-query) SourceQuery
    (s/optional-key :source-table) su/IntGreaterThanZero
    (s/optional-key :aggregation)  [Aggregation]
    (s/optional-key :breakout)     [Field]
    (s/optional-key :expressions)  {s/Keyword ExpressionDef} ; TODO - I think expressions keys should be strings
    (s/optional-key :fields)       [Field]
    (s/optional-key :filter)       Filter
    (s/optional-key :limit)        su/IntGreaterThanZero
    (s/optional-key :order-by)     s/Any
    (s/optional-key :page)         {:page  su/IntGreaterThanOrEqualToZero
                                    :items su/IntGreaterThanZero}}
   (fn [query]
     (core/= 1 (core/count (select-keys query [:source-query :source-table]))))
   "Query must specify either `:source-table` or `:source-query`, but not both."))

(def ^:private Parameter
  "Schema for a valid, normalized query parameter."
  s/Any) ; s/Any for now until we move over the stuff from the parameters middleware

(def Query
  "Schema for an [outer] query, e.g. the sort of thing you'd pass to the query processor or save in
  `Card.dataset_query`."
  (s/constrained
   {:database                    su/IntGreaterThanZero
    :type                        (s/enum :query :native)
    (s/optional-key :native)     NativeQuery
    (s/optional-key :query)      MBQLQuery
    (s/optional-key :parameters) [Parameter]}
   (fn [{native :native, mbql :query, query-type :type}]
     (case query-type
       :native (and native (not mbql))
       :query  (and mbql   (not native))))
   "Native queries should specify `:native` but not `:query`; MBQL queries should specify `:query` but not `:native`."))
