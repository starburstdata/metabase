(ns metabase.driver.implementation.execute
  "Execute implementation for Trino JDBC driver."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [java-time :as t]
            [metabase.driver.implementation.sync :refer [trino-type->base-type]]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql.parameters.substitution :as sql.params.substitution]
            [metabase.query-processor.timezone :as qp.timezone]
            [metabase.util.date-2 :as u.date]
            [metabase.util.i18n :refer [trs]])
  (:import com.mchange.v2.c3p0.C3P0ProxyConnection
           io.trino.jdbc.TrinoConnection
           [java.sql ResultSet Time Types Connection PreparedStatement ResultSetMetaData]
           java.sql.Time
           [java.time LocalTime OffsetDateTime ZonedDateTime LocalDateTime OffsetTime]
           java.time.format.DateTimeFormatter
           [java.time.temporal ChronoField Temporal]))

(defn- ^TrinoConnection pooled-conn->trino-conn
  "Unwraps the C3P0 `pooled-conn` and returns the underlying `TrinoConnection` it holds."
  [^C3P0ProxyConnection pooled-conn]
  (.unwrap pooled-conn TrinoConnection))

(defn- ^TrinoConnection rs->trino-conn
  "Returns the `TrinoConnection` associated with the given `ResultSet` `rs`."
  [^ResultSet rs]
  (-> (.. rs getStatement getConnection)
      pooled-conn->trino-conn))

(defmethod sql-jdbc.execute/connection-with-timezone :trino
  [driver database ^String timezone-id]
  ;; Presto supports setting the session timezone via a `TrinoConnection` instance method. Under the covers,
  ;; this is equivalent to the `X-Presto-Time-Zone` header in the HTTP request (i.e. the `:presto` driver)
  (let [conn            (.getConnection (sql-jdbc.execute/datasource-with-diagnostic-info! driver database))
        underlying-conn (pooled-conn->trino-conn conn)]
    (try
      (sql-jdbc.execute/set-best-transaction-level! driver conn)
      (when-not (str/blank? timezone-id)
        ;; set session time zone if defined
        (.setTimeZoneId underlying-conn timezone-id))
      (try
        (.setReadOnly conn true)
        (catch Throwable e
          (log/debug e (trs "Error setting connection to read-only"))))
      ;; as with statement and prepared-statement, cannot set holdability on the connection level
      conn
      (catch Throwable e
        (.close conn)
        (throw e)))))

(defmethod sql-jdbc.execute/read-column-thunk [:trino Types/TIMESTAMP]
  [_ ^ResultSet rset _ ^Integer i]
  (let [zone     (.getTimeZoneId (rs->trino-conn rset))]
    (fn []
      (when-let [s (.getString rset i)]
        (when-let [t (u.date/parse s)]
          (cond
            (or (instance? OffsetDateTime t)
                (instance? ZonedDateTime t))
            (-> (t/offset-date-time t)
              ;; tests are expecting this to be in the UTC offset, so convert to that
                (t/with-offset-same-instant (t/zone-offset 0)))

            ;; presto "helpfully" returns local results already adjusted to session time zone offset for us, e.g.
            ;; '2021-06-15T00:00:00' becomes '2021-06-15T07:00:00' if the session timezone is US/Pacific. Undo the
            ;; madness and convert back to UTC
            zone
            (t/local-date-time t)
            :else
            t))))))

;; (defmethod sql-jdbc.execute/read-column-thunk [:trino Types/TIMESTAMP]
;;   [_ ^ResultSet rset _ ^long i]
;;   (println "Custom Thunk Types/TIMESTAMP...")
;;   (fn []
;;     (let [t (.getObject rset i java.sql.Timestamp)
;;           instant (.toInstant t)]
;;       (.atOffset instant (t/zone-offset)))))

(defmethod sql-jdbc.execute/read-column-thunk [:trino Types/TIMESTAMP_WITH_TIMEZONE]
  [_ ^ResultSet rset _ ^long i]
  (fn []
    (let [t (.getObject rset i java.sql.Timestamp)
          instant (.toInstant t)]
      (.atOffset instant (t/zone-offset)))))

(defn- ^LocalTime sql-time->local-time
  "Converts the given instance of `java.sql.Time` into a `java.time.LocalTime`, including milliseconds. Needed for
  similar reasons as `set-time-param` above."
  [^Time sql-time]
  ;; Java 11 adds a simpler `ofInstant` method, but since we need to run on JDK 8, we can't use it
  ;; https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalTime.html#ofInstant(java.time.Instant,java.time.ZoneId)
  (let [^LocalTime lt (t/local-time sql-time)
        ^Long millis  (mod (.getTime sql-time) 1000)]
    (.with lt ChronoField/MILLI_OF_SECOND millis)))

(defmethod sql-jdbc.execute/read-column-thunk [:trino Types/TIME_WITH_TIMEZONE]
  [_ rs _ i]
  (fn []
    (let [local-time (-> (.getTime rs i)
                         sql-time->local-time)]
      (t/offset-time
       local-time
       (t/zone-offset 0)))))


(defmethod sql-jdbc.execute/prepared-statement :trino
  [driver ^Connection conn ^String sql params]
  ;; with Presto JDBC driver, result set holdability must be HOLD_CURSORS_OVER_COMMIT
  ;; defining this method simply to omit setting the holdability
  (let [stmt (.prepareStatement conn
                                sql
                                ResultSet/TYPE_FORWARD_ONLY
                                ResultSet/CONCUR_READ_ONLY)]
    (try
      (try
        (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
        (catch Throwable e
          (log/debug e (trs "Error setting prepared statement fetch direction to FETCH_FORWARD"))))
      (sql-jdbc.execute/set-parameters! driver stmt params)
      stmt
      (catch Throwable e
        (.close stmt)
        (throw e)))))


(defmethod sql-jdbc.execute/statement :trino
  [_ ^Connection conn]
  ;; and similarly for statement (do not set holdability)
  (let [stmt (.createStatement conn
                               ResultSet/TYPE_FORWARD_ONLY
                               ResultSet/CONCUR_READ_ONLY)]
    (try
      (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
      (catch Throwable e
        (log/debug e (trs "Error setting statement fetch direction to FETCH_FORWARD"))))
    stmt))

(defn- date-time->substitution [ts-str]
  (sql.params.substitution/make-stmt-subs "from_iso8601_timestamp(?)" [ts-str]))

(defmethod sql.params.substitution/->prepared-substitution [:trino ZonedDateTime]
  [_ ^ZonedDateTime t]
  ;; for native query parameter substitution, in order to not conflict with the `PrestoConnection` session time zone
  ;; (which was set via report time zone), it is necessary to use the `from_iso8601_timestamp` function on the string
  ;; representation of the `ZonedDateTime` instance, but converted to the report time zone
  #_(date-time->substitution (.format (t/offset-date-time (t/local-date-time t) (t/zone-offset 0)) DateTimeFormatter/ISO_OFFSET_DATE_TIME))
  (let [report-zone       (qp.timezone/report-timezone-id-if-supported :trino)
        ^ZonedDateTime ts (if (str/blank? report-zone) t (t/with-zone-same-instant t (t/zone-id report-zone)))]
    ;; the `from_iso8601_timestamp` only accepts timestamps with an offset (not a zone ID), so only format with offset
    (date-time->substitution (.format ts DateTimeFormatter/ISO_OFFSET_DATE_TIME))))

(defmethod sql.params.substitution/->prepared-substitution [:trino LocalDateTime]
  [_ ^LocalDateTime t]
  ;; similar to above implementation, but for `LocalDateTime`
  ;; when Presto parses this, it will account for session (report) time zone
  (date-time->substitution (.format t DateTimeFormatter/ISO_LOCAL_DATE_TIME)))

(defmethod sql.params.substitution/->prepared-substitution [:trino OffsetDateTime]
  [_ ^OffsetDateTime t]
  ;; similar to above implementation, but for `ZonedDateTime`
  ;; when Presto parses this, it will account for session (report) time zone
  (date-time->substitution (.format t DateTimeFormatter/ISO_OFFSET_DATE_TIME)))

(defn- set-time-param
  "Converts the given instance of `java.time.temporal`, assumed to be a time (either `LocalTime` or `OffsetTime`)
  into a `java.sql.Time`, including milliseconds, and sets the result as a parameter of the `PreparedStatement` `ps`
  at index `i`."
  [^PreparedStatement ps ^Integer i ^Temporal t]
  ;; for some reason, `java-time` can't handle passing millis to java.sql.Time, so this is the most straightforward way
  ;; I could find to do it
  ;; reported as https://github.com/dm3/clojure.java-time/issues/74
  (let [millis-of-day (.get t ChronoField/MILLI_OF_DAY)]
    (.setTime ps i (Time. millis-of-day))))

(defmethod sql-jdbc.execute/set-parameter [:trino OffsetTime]
  [_ ^PreparedStatement ps ^Integer i t]
  ;; necessary because `PrestoPreparedStatement` does not implement the `setTime` overload having the final `Calendar`
  ;; param
  (let [adjusted-tz (t/with-offset-same-instant t (t/zone-offset 0))]
    (set-time-param ps i adjusted-tz)))

(defmethod sql-jdbc.execute/set-parameter [:trino LocalTime]
  [_ ^PreparedStatement ps ^Integer i t]
  ;; same rationale as above
  (set-time-param ps i t))

(defn- ^LocalTime sql-time->local-time
  "Converts the given instance of `java.sql.Time` into a `java.time.LocalTime`, including milliseconds. Needed for
  similar reasons as `set-time-param` above."
  [^Time sql-time]
  ;; Java 11 adds a simpler `ofInstant` method, but since we need to run on JDK 8, we can't use it
  ;; https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalTime.html#ofInstant(java.time.Instant,java.time.ZoneId)
  (let [^LocalTime lt (t/local-time sql-time)
        ^Long millis  (mod (.getTime sql-time) 1000)]
    (.with lt ChronoField/MILLI_OF_SECOND millis)))

(defmethod sql-jdbc.execute/read-column-thunk [:trino Types/TIME]
  [_ ^ResultSet rs ^ResultSetMetaData rs-meta ^Integer i]
  (let [type-name  (.getColumnTypeName rs-meta i)
        base-type  (trino-type->base-type type-name)
        with-tz?   (isa? base-type :type/TimeWithTZ)]
    (fn []
      (let [local-time (-> (.getTime rs i)
                           sql-time->local-time)]
        ;; for both `time` and `time with time zone`, the JDBC type reported by the driver is `Types/TIME`, hence
        ;; we also need to check the column type name to differentiate between them here
        (if with-tz?
          ;; even though this value is a `LocalTime`, the base-type is time with time zone, so we need to shift it back to
          ;; the UTC (0) offset
          (t/offset-time
           local-time
           (t/zone-offset 0))
          ;; else the base-type is time without time zone, so just return the local-time value
          local-time)))))