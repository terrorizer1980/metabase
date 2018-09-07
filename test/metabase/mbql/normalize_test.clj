(ns metabase.mbql.normalize-test
  (:require [expectations :refer :all]
            [metabase.mbql.normalize :as normalize]))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                NORMALIZE TOKENS                                                |
;;; +----------------------------------------------------------------------------------------------------------------+

;; Query type should get normalized
(expect
  {:type :native}
  (#'normalize/normalize-tokens {:type "NATIVE"}))

;; native queries should NOT get normalized
(expect
  {:type :native, :native {:query "SELECT COUNT(*) FROM CANS;"}}
  (#'normalize/normalize-tokens {:type "NATIVE", :native {"QUERY" "SELECT COUNT(*) FROM CANS;"}}))

(expect
  {:native {:query {:NAME        "FAKE_QUERY"
                    :description "Theoretical fake query in a JSON-based query lang"}}}
  (#'normalize/normalize-tokens {:native {:query {:NAME        "FAKE_QUERY"
                                                  :description "Theoretical fake query in a JSON-based query lang"}}}))

;; do aggregations get normalized?
(expect
  {:query {:aggregation :rows}}
  (#'normalize/normalize-tokens {:query {"AGGREGATION" "ROWS"}}))

(expect
  {:query {:aggregation [:rows]}}
  (#'normalize/normalize-tokens {:query {"AGGREGATION" ["ROWS"]}}))

(expect
 {:query {:aggregation [:count 10]}}
 (#'normalize/normalize-tokens {:query {"AGGREGATION" ["COUNT" 10]}}))

(expect
  {:query {:aggregation [[:count 10]]}}
  (#'normalize/normalize-tokens {:query {"AGGREGATION" [["COUNT" 10]]}}))

;; make sure we normalize ag tokens properly when there's wacky MBQL 95 ag syntax
(expect
  {:query {:aggregation [:rows :count]}}
  (#'normalize/normalize-tokens {:query {:aggregation ["rows" "count"]}}))

(expect
  {:query {:aggregation [:count :count]}}
  (#'normalize/normalize-tokens {:query {:aggregation ["count" "count"]}}))

;; don't normalize names of expression refs!
(expect
  {:query {:aggregation [:count [:count [:expression "ABCDEF"]]]}}
  (#'normalize/normalize-tokens {:query {:aggregation ["count" ["count" ["expression" "ABCDEF"]]]}}))

;; or field literals!
(expect
  {:query {:aggregation [:count [:count [:field-literal "ABCDEF" :type/Text]]]}}
  (#'normalize/normalize-tokens {:query {:aggregation ["count" ["count" ["field_literal" "ABCDEF" "type/Text"]]]}}))

;; event if you trry your best to break things it should handle it
(expect
  {:query {:aggregation [:count [:sum 10] [:count 20] :count]}}
  (#'normalize/normalize-tokens {:query {:aggregation ["count" ["sum" 10] ["count" 20] "count"]}}))

;; are expression names exempt from lisp-casing/lower-casing?
(expect
  {:query {:expressions {:sales_tax [:- [:field-id 10] [:field-id 20]]}}}
  (#'normalize/normalize-tokens {"query" {"expressions" {:sales_tax ["-" ["field-id" 10] ["field-id" 20]]}}}))

;; expression names should always be keywords
(expect
  {:query {:expressions {:sales_tax [:- [:field-id 10] [:field-id 20]]}}}
  (#'normalize/normalize-tokens {"query" {"expressions" {:sales_tax ["-" ["field-id" 10] ["field-id" 20]]}}}))

;; expression references should be exempt too
(expect
  {:order-by [[:desc [:expression "SALES_TAX"]]]}
  (#'normalize/normalize-tokens {:order-by [[:desc [:expression "SALES_TAX"]]]}) )

;; ... but they should be converted to strings if passed in as a KW for some reason. Make sure we preserve namespace!
(expect
  {:order-by [[:desc [:expression "SALES/TAX"]]]}
  (#'normalize/normalize-tokens {:order-by [[:desc ["expression" :SALES/TAX]]]}))

;; field literals should be exempt too
(expect
  {:order-by [[:desc [:field-literal "SALES_TAX" :type/Number]]]}
  (#'normalize/normalize-tokens {:order-by [[:desc [:field-literal "SALES_TAX" :type/Number]]]}) )

;; ... but they should be converted to strings if passed in as a KW for some reason
(expect
  {:order-by [[:desc [:field-literal "SALES/TAX" :type/Number]]]}
  (#'normalize/normalize-tokens {:order-by [[:desc ["field_literal" :SALES/TAX "type/Number"]]]}))

;; does order-by get properly normalized?
(expect
  {:query {:order-by [[10 :asc]]}}
  (#'normalize/normalize-tokens {:query {"ORDER_BY" [[10 "ASC"]]}}))

(expect
  {:query {:order-by [[:asc 10]]}}
  (#'normalize/normalize-tokens {:query {"ORDER_BY" [["ASC" 10]]}}))

(expect
  {:query {:order-by [[[:field-id 10] :asc]]}}
  (#'normalize/normalize-tokens {:query {"ORDER_BY" [[["field_id" 10] "ASC"]]}}))

(expect
  {:query {:order-by [[:desc [:field-id 10]]]}}
  (#'normalize/normalize-tokens {:query {"ORDER_BY" [["DESC" ["field_id" 10]]]}}))

;; the unit & amount in time interval clauses should get normalized
(expect
  {:query {:filter [:time-interval 10 :current :day]}}
  (#'normalize/normalize-tokens {:query {"FILTER" ["time-interval" 10 "current" "day"]}}))

;; but amount should not get normalized if it's an integer
(expect
  {:query {:filter [:time-interval 10 -10 :day]}}
  (#'normalize/normalize-tokens {:query {"FILTER" ["time-interval" 10 -10 "day"]}}))

;; the unit in relative datetime clauses should get normalized
(expect
  {:query {:filter [:= [:field-id 10] [:relative-datetime -31 :day]]}}
  (#'normalize/normalize-tokens {:query {"FILTER" ["=" [:field-id 10] ["RELATIVE_DATETIME" -31 "DAY"]]}}))

;; should work if we do [:relative-datetime :current] as well
(expect
  {:query {:filter [:= [:field-id 10] [:relative-datetime :current]]}}
  (#'normalize/normalize-tokens {:query {"FILTER" ["=" [:field-id 10] ["RELATIVE_DATETIME" "CURRENT"]]}}))

;; and in datetime-field clauses (MBQL 98)
(expect
  {:query {:filter [:= [:datetime-field [:field-id 10] :day] "2018-09-05"]}}
  (#'normalize/normalize-tokens {:query {"FILTER" ["=" [:datetime-field ["field_id" 10] "day"] "2018-09-05"]}}))

;; (or in long-since-deprecated MBQL 95 format)
(expect
  {:query {:filter [:= [:datetime-field 10 :as :day] "2018-09-05"]}}
  (#'normalize/normalize-tokens {:query {"FILTER" ["=" [:datetime-field 10 "as" "day"] "2018-09-05"]}}))

;; if string filters have an options map that should get normalized
(expect
  {:query {:filter [:starts-with 10 "ABC" {:case-sensitive true}]}}
  (#'normalize/normalize-tokens {:query {"FILTER" ["starts_with" 10 "ABC" {"case_sensitive" true}]}}))

;; make sure we're not running around trying to normalize the type in native query params
(expect
  {:type       :native
   :parameters [{:type   "date/range"
                 :target [:dimension [:template-tag "checkin_date"]]
                 :value  "2015-04-01~2015-05-01"}]}
  (#'normalize/normalize-tokens {:type       :native
                                 :parameters [{:type   "date/range"
                                               :target [:dimension [:template-tag "checkin_date"]]
                                               :value  "2015-04-01~2015-05-01"}]}))

;; oh yeah, also we don't want to go around trying to normalize template-tag names
(expect
  {:type   :native
   :native {:query         "SELECT COUNT(*) FROM \"PUBLIC\".\"CHECKINS\" WHERE {{checkin_date}}"
            :template-tags {:checkin_date {:name         "checkin_date"
                                           :display_name "Checkin Date"
                                           :type         "dimension",
                                           :dimension    [:field-id 14]}}}}
  (#'normalize/normalize-tokens
   {:type   "native"
    :native {:query         "SELECT COUNT(*) FROM \"PUBLIC\".\"CHECKINS\" WHERE {{checkin_date}}"
             :template_tags {:checkin_date {:name         "checkin_date"
                                            :display_name "Checkin Date"
                                            :type         "dimension",
                                            :dimension    ["field-id" 14]}}}}))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  CANONICALIZE                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

(expect
  [:field-id 10]
  (#'normalize/wrap-implicit-field-id 10))

(expect
  {:query {:aggregation [[:count [:field-id 10]]]}}
  (#'normalize/canonicalize {:query {:aggregation [:count 10]}}))

(expect
  {:query {:aggregation [[:count]]}}
  (#'normalize/canonicalize {:query {:aggregation [:count]}}))

(expect
  {:query {:aggregation [[:count [:field-id 1000]]]}}
  (#'normalize/canonicalize {:query {:aggregation [:count [:field-id 1000]]}}))

;; :rows aggregation type, being deprecated since FOREVER, should just get removed
(expect
  {:query {:aggregation []}}
  (#'normalize/canonicalize {:query {:aggregation [:rows]}}))

(expect
  {:query {:aggregation []}}
  (#'normalize/canonicalize {:query {:aggregation :rows}}))

;; if just a single aggregation is supplied it should always be converted to new-style multiple-aggregation syntax
(expect
  {:query {:aggregation [[:count]]}}
  (#'normalize/canonicalize {:query {:aggregation :count}}))

;; Raw Field IDs in an aggregation should get wrapped in [:field-id] clause
(expect
  {:query {:aggregation [[:count [:field-id 10]]]}}
  (#'normalize/canonicalize {:query {:aggregation [:count 10]}}))

;; make sure we handle single :count with :field-id correctly
(expect
  {:query {:aggregation [[:count [:field-id 10]]]}}
  (#'normalize/canonicalize {:query {:aggregation [:count [:field-id 10]]}}))

;; make sure for multiple aggregations we can handle `:count` that doesn't appear wrapped in brackets
(expect
  {:query {:aggregation [[:count] [:sum [:field-id 10]]]}}
  (#'normalize/canonicalize {:query {:aggregation [:count [:sum 10]]}}))

;; this doesn't make sense, but make sure if someone specifies a `:rows` ag and another one we don't end up with a
;; `nil` in the ags list
(expect
  {:query {:aggregation [[:count]]}}
  (#'normalize/canonicalize {:query {:aggregation [:rows :count]}}))

;; another stupid aggregation that we need to be able to handle
(expect
  {:query {:aggregation [[:count] [:count]]}}
  (#'normalize/canonicalize {:query {:aggregation [:count :count]}}))

;; a mix of unwrapped & wrapped should still work
(expect
  {:query {:aggregation [[:count] [:sum [:field-id 10]] [:count [:field-id 20]] [:count]]}}
  (#'normalize/canonicalize {:query {:aggregation [:count [:sum 10] [:count 20] :count]}}))

;; make sure we can deal with *named* aggregations!
(expect
  {:query {:aggregation [:named [:sum [:field-id 10]] "Sum *TEN*"]}}
  (#'normalize/canonicalize {:query {:aggregation [:named [:sum 10] "Sum *TEN*"]}}))


;; implicit Field IDs should get wrapped in [:field-id] in :breakout
(expect
  {:query {:breakout [[:field-id 10]]}}
  (#'normalize/canonicalize {:query {:breakout [10]}}))

(expect
  {:query {:breakout [[:field-id 10] [:field-id 20]]}}
  (#'normalize/canonicalize {:query {:breakout [10 20]}}))

(expect
  {:query {:breakout [[:field-id 1000]]}}
  (#'normalize/canonicalize {:query {:breakout [[:field-id 1000]]}}))

(expect
  {:query {:fields [[:field-id 10]]}}
  (#'normalize/canonicalize {:query {:fields [10]}}))

;; implicit Field IDs should get wrapped in [:field-id] in :fields
(expect
  {:query {:fields [[:field-id 10] [:field-id 20]]}}
  (#'normalize/canonicalize {:query {:fields [10 20]}}))

(expect
  {:query {:fields [[:field-id 1000]]}}
  (#'normalize/canonicalize {:query {:fields [[:field-id 1000]]}}))

;; implicit Field IDs should get wrapped in [:field-id] in filters
(expect
  {:query {:filter [:= [:field-id 10] 20]}}
  (#'normalize/canonicalize {:query {:filter [:= 10 20]}}))

(expect
  {:query {:filter [:and [:= [:field-id 10] 20] [:= [:field-id 20] 30]]}}
  (#'normalize/canonicalize {:query {:filter [:and
                                                      [:= 10 20]
                                                      [:= 20 30]]}}))

(expect
  {:query {:filter [:between [:field-id 10] 20 30]}}
  (#'normalize/canonicalize {:query {:filter [:between 10 20 30]}}))

;; compound filters with only one arg should get automatically de-compounded
(expect
  {:query {:filter [:= [:field-id 100]]}}
  (#'normalize/canonicalize {:query {:filter [:and [:= 100]]}}))

(expect
  {:query {:filter [:= [:field-id 100]]}}
  (#'normalize/canonicalize {:query {:filter [:or [:= 100]]}}))

;; string filters with empty options maps should get case-sensitive: true added
(expect
  {:query {:filter [:starts-with [:field-id 10] "ABC" {:case-sensitive true}]}}
  (#'normalize/canonicalize {:query {:filter [:starts-with 10 "ABC"]}}))

;; make sure we don't overwrite options if specified
(expect
  {:query {:filter [:contains [:field-id 10] "ABC" {:case-sensitive false}]}}
  (#'normalize/canonicalize {:query {:filter [:contains 10 "ABC" {:case-sensitive false}]}}))

;; if you try to pass in invalid options, toss them out
(expect
  {:query {:filter [:starts-with [:field-id 10] "ABC" {:case-sensitive true}]}}
  (#'normalize/canonicalize {:query {:filter [:starts-with 10 "ABC" {:cans-count 2}]}}))

;; ORDER BY: MBQL 95 [field direction] should get translated to MBQL 98 [direction field]
(expect
  {:query {:order-by [[:asc [:field-id 10]]]}}
  (#'normalize/canonicalize {:query {:order-by [[[:field-id 10] :asc]]}}))

;; MBQL 95 old names should be handled
(expect
  {:query {:order-by [[:asc [:field-id 10]]]}}
  (#'normalize/canonicalize {:query {:order-by [[10 :ascending]]}}))

;; field-id should be added if needed
(expect
  {:query {:order-by [[:asc [:field-id 10]]]}}
  (#'normalize/canonicalize {:query {:order-by [[10 :asc]]}}))

(expect
  {:query {:order-by [[:asc [:field-id 10]]]}}
  (#'normalize/canonicalize {:query {:order-by [[:asc 10]]}}))

;; fk-> clauses should get the field-id treatment
(expect
  {:query {:filter [:= [:fk-> [:field-id 10] [:field-id 20]] "ABC"]}}
  (#'normalize/canonicalize {:query {:filter [:= [:fk-> 10 20] "ABC"]}}))

;; as should datetime-field clauses...
(expect
  {:query {:filter [:= [:datetime-field [:field-id 10] :day] "2018-09-05"]}}
  (#'normalize/canonicalize {:query {:filter [:= [:datetime-field 10 :day] "2018-09-05"]}}))

;; MBQL 95 datetime-field clauses ([:datetime-field <field> :as <unit>]) should get converted to MBQL 98
(expect
  {:query {:filter [:= [:datetime-field [:field-id 10] :day] "2018-09-05"]}}
  (#'normalize/canonicalize {:query {:filter [:= [:datetime-field 10 :as :day] "2018-09-05"]}}))

;; if someone is dumb and passes something like a field-literal inside a field-id, fix it for them.
(expect
  {:query {:filter [:= [:field-literal "my_field" "type/Number"] 10]}}
  (#'normalize/canonicalize {:query {:filter [:= [:field-id [:field-literal "my_field" "type/Number"]] 10]}}))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                              REMOVE EMPTY CLAUSES                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

;; empty sequences should get removed
(expect
  {:y [100]}
  (#'normalize/remove-empty-clauses {:x [], :y [100]}))

;; nil values should get removed
(expect
  {:y 100}
  (#'normalize/remove-empty-clauses {:x nil, :y 100}))

;; sequences containing only nil should get removed
(expect
  {:a [nil 100]}
  (#'normalize/remove-empty-clauses {:a [nil 100], :b [nil nil]}))

;; empty maps should get removed
(expect
  {:a {:b 100}}
  (#'normalize/remove-empty-clauses {:a {:b 100}, :c {}}))

(expect
  {:a {:b 100}}
  (#'normalize/remove-empty-clauses {:a {:b 100}, :c {:d nil}}))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                            PUTTING IT ALL TOGETHER                                             |
;;; +----------------------------------------------------------------------------------------------------------------+

;; With an ugly MBQL 95 query, does everything get normalized nicely?
(expect
  {:type :query
   :query  {:source-table 10
            :breakout     [[:field-id 10] [:field-id 20]]
            :filter       [:= [:field-id 10] [:datetime-field [:field-id 20] :day]]
            :order-by     [[:desc [:field-id 10]]]}}
  (normalize/normalize {:type  "query"
                        :query {"source_table" 10
                                "AGGREGATION"  "ROWS"
                                "breakout"     [10 20]
                                "filter"       ["and" ["=" 10 ["datetime-field" 20 "as" "day"]]]
                                "order-by"     [[10 "desc"]]}}))
