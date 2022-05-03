(ns metabase.driver.implementation.query-processor  "Query processor implementations for Trino JDBC driver."
  (:require [honeysql.core :as hsql]
            [honeysql.format :as hformat]
            [honeysql.helpers :as hh]
            [java-time :as t]
            [metabase.driver.sql.query-processor :as sql.qp]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.util :as u]
            [metabase.util.date-2 :as u.date]
            [metabase.util.honeysql-extensions :as hx])
    (:import [java.time OffsetDateTime ZonedDateTime]))

(def ^:private ^:const timestamp-with-time-zone-db-type "timestamp with time zone")

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                          Misc Implementations                                                       |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/->float :trino
  [_ value]
  (hx/cast :double value))

(defmethod hformat/fn-handler (u/qualified-name ::mod)
  [_ x y]
  ;; Presto mod is a function like mod(x, y) rather than an operator like x mod y
  (format "mod(%s, %s)" (hformat/to-sql x) (hformat/to-sql y)))

(defmethod sql.qp/add-interval-honeysql-form :trino
  [_ hsql-form amount unit]
  (hsql/call :date_add (hx/literal unit) amount hsql-form))

(defmethod sql.qp/apply-top-level-clause [:trino :page]
  [_ _ honeysql-query {{:keys [items page]} :page}]
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can simply use limit
      (hh/limit honeysql-query items)
      ;; if we need to do an offset we have to do nesting to generate a row number and where on that
      (let [over-clause (format "row_number() OVER (%s)"
                                (first (hsql/format (select-keys honeysql-query [:order-by])
                                                    :allow-dashed-names? true
                                                    :quoting :ansi)))]
        (-> (apply hh/select (map last (:select honeysql-query)))
            (hh/from (hh/merge-select honeysql-query [(hsql/raw over-clause) :__rownum__]))
            (hh/where [:> :__rownum__ offset])
            (hh/limit items))))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                          Temporal Casting                                                       |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/cast-temporal-string [:trino :Coercion/YYYYMMDDHHMMSSString->Temporal]
  [_ _coercion-strategy expr]
  (hsql/call :date_parse expr (hx/literal "%Y%m%d%H%i%s")))

(defmethod sql.qp/cast-temporal-byte [:trino :Coercion/YYYYMMDDHHMMSSBytes->Temporal]
  [driver _coercion-strategy expr]
  (sql.qp/cast-temporal-string driver :Coercion/YYYYMMDDHHMMSSString->Temporal
                               (hsql/call :from_utf8 expr)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                          Date Truncation                                                       |
;;; +----------------------------------------------------------------------------------------------------------------+

(defrecord AtTimeZone
  ;; record type to support applying Presto's `AT TIME ZONE` operator to an expression
           [expr zone]
  hformat/ToSql
  (to-sql [_]
    (format "%s AT TIME ZONE %s"
            (hformat/to-sql expr)
            (hformat/to-sql (hx/literal zone)))))

(defn- in-report-zone
  "Returns a HoneySQL form to interpret the `expr` (a temporal value) in the current report time zone, via Trino's
  `AT TIME ZONE` operator. See https://trino.io/docs/current/functions/datetime.html#time-zone-conversion"
  [expr]
  (let [report-zone (qp.timezone/report-timezone-id-if-supported :trino)
        ;; if the expression itself has type info, use that, or else use a parent expression's type info if defined
        type-info   (hx/type-info expr)
        db-type     (hx/type-info->db-type type-info)]
    (if (and ;; AT TIME ZONE is only valid on these Trino types; if applied to something else (ex: `date`), then
             ;; an error will be thrown by the query analyzer
         (contains? #{"timestamp" "timestamp with time zone" "time" "time with time zone"} db-type)
             ;; if one has already been set, don't do so again
         (not (::in-report-zone? (meta expr)))
         report-zone)
      (-> (hx/with-database-type-info (->AtTimeZone expr report-zone) timestamp-with-time-zone-db-type)
          (vary-meta assoc ::in-report-zone? true))
      expr)))

;; most date extraction and bucketing functions need to account for report timezone

(defmethod sql.qp/date [:trino :default]
  [_ _ expr]
  expr)

(defmethod sql.qp/date [:trino :minute]
  [_ _ expr]
  (hsql/call :date_trunc (hx/literal :minute) (in-report-zone expr)))

(defmethod sql.qp/date [:trino :minute-of-hour]
  [_ _ expr]
  (hsql/call :minute (in-report-zone expr)))

(defmethod sql.qp/date [:trino :hour]
  [_ _ expr]
  (hsql/call :date_trunc (hx/literal :hour) (in-report-zone expr)))

(defmethod sql.qp/date [:trino :hour-of-day]
  [_ _ expr]
  (hsql/call :hour (in-report-zone expr)))

(defmethod sql.qp/date [:trino :day]
  [_ _ expr]
  (hsql/call :date (in-report-zone expr)))

(defmethod sql.qp/date [:trino :day-of-week]
  [_ _ expr]
  (sql.qp/adjust-day-of-week :trino (hsql/call :day_of_week (in-report-zone expr))))

(defmethod sql.qp/date [:trino :day-of-month]
  [_ _ expr]
  (hsql/call :day (in-report-zone expr)))

(defmethod sql.qp/date [:trino :day-of-year]
  [_ _ expr]
  (hsql/call :day_of_year (in-report-zone expr)))

(defmethod sql.qp/date [:trino :week]
  [_ _ expr]
  (sql.qp/adjust-start-of-week :trino (partial hsql/call :date_trunc (hx/literal :week)) (in-report-zone expr)))

(defmethod sql.qp/date [:trino :month]
  [_ _ expr]
  (hsql/call :date_trunc (hx/literal :month) (in-report-zone expr)))

(defmethod sql.qp/date [:trino :month-of-year]
  [_ _ expr]
  (hsql/call :month (in-report-zone expr)))

(defmethod sql.qp/date [:trino :quarter]
  [_ _ expr]
  (hsql/call :date_trunc (hx/literal :quarter) (in-report-zone expr)))

(defmethod sql.qp/date [:trino :quarter-of-year]
  [_ _ expr]
  (hsql/call :quarter (in-report-zone expr)))

(defmethod sql.qp/date [:trino :year]
  [_ _ expr]
  (hsql/call :date_trunc (hx/literal :year) (in-report-zone expr)))

(defmethod sql.qp/current-datetime-honeysql-form :trino
  [_]
  ;; the current_timestamp in Presto returns a `timestamp with time zone`, so this needs to be overridden
  (hx/with-type-info :%now {::hx/database-type timestamp-with-time-zone-db-type}))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                          Custom HoneySQL Clause Impls                                          |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/->honeysql [:trino Boolean]
  [_ bool]
  (hsql/raw (if bool "TRUE" "FALSE")))

;; (defmethod sql.qp/->honeysql [:trino :time]
;;   [_ [_ t]]
;;   ;; Convert t to locale time, then format as sql. Then add cast.
;;   (hx/cast :time (u.date/format-sql (t/local-time t))))

(defmethod sql.qp/->honeysql [:trino :regex-match-first]
  [driver [_ arg pattern]]
  (hsql/call :regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)))

(defmethod sql.qp/->honeysql [:trino :median]
  [driver [_ arg]]
  (hsql/call :approx_percentile (sql.qp/->honeysql driver arg) 0.5))

(defmethod sql.qp/->honeysql [:trino :percentile]
  [driver [_ arg p]]
  (hsql/call :approx_percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)))

(defmethod sql.qp/->honeysql [:trino :log]
  [driver [_ field]]
  ;; recent Presto versions have a `log10` function (not `log`)
  (hsql/call :log10 (sql.qp/->honeysql driver field)))

(defmethod sql.qp/->honeysql [:trino :count-where]
  [driver [_ pred]]
  ;; Presto will use the precision given here in the final expression, which chops off digits
  ;; need to explicitly provide two digits after the decimal
  (sql.qp/->honeysql driver [:sum-where 1.00M pred]))

(defmethod sql.qp/->honeysql [:trino :time]
  [_ [_ t]]
  ;; make time in UTC to avoid any interpretation by Trino in the connection (i.e. report) time zone
  (hx/cast "time with time zone" (u.date/format-sql (t/offset-time (t/local-time t) 0))))

(defmethod sql.qp/->honeysql [:trino ZonedDateTime]
  [_ ^ZonedDateTime t]
  ;; use the Trino cast to `timestamp with time zone` operation to interpret in the correct TZ, regardless of
  ;; connection zone
  (hx/cast timestamp-with-time-zone-db-type (u.date/format-sql t)))

(defmethod sql.qp/->honeysql [:trino OffsetDateTime]
  [_ ^OffsetDateTime t]
  ;; use the Presto cast to `timestamp with time zone` operation to interpret in the correct TZ, regardless of
  ;; connection zone
  (hx/cast timestamp-with-time-zone-db-type (u.date/format-sql t)))

(defmethod sql.qp/unix-timestamp->honeysql [:trino :seconds]
  [_ _ expr]
  (let [report-zone (qp.timezone/report-timezone-id-if-supported :trino)]
    (hsql/call :from_unixtime expr (hx/literal (or report-zone "UTC")))))

(defmethod sql.qp/unix-timestamp->honeysql [:trino :milliseconds]
  [_ _ expr]
  ;; from_unixtime doesn't support milliseconds directly, but we can add them back in
  (let [report-zone (qp.timezone/report-timezone-id-if-supported :trino)
        millis      (hsql/call (u/qualified-name ::mod) expr 1000)]
    (hsql/call :date_add
               (hx/literal "millisecond")
               millis
               (hsql/call :from_unixtime (hsql/call :/ expr 1000) (hx/literal (or report-zone "UTC"))))))
