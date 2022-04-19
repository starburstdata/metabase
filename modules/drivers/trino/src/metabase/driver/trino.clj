(ns metabase.driver.trino
  "Trino JDBC driver."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [java-time :as t]
            [metabase.db.spec :as db.spec]
            [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.driver.sql-jdbc.execute.legacy-impl :as legacy]
            [metabase.util.date-2 :as u.date]
            [metabase.util.i18n :refer [trs]])
  
  (:import com.mchange.v2.c3p0.C3P0ProxyConnection
           io.trino.jdbc.TrinoConnection
           [java.sql ResultSet Time Types]
           [java.time LocalTime OffsetDateTime ZonedDateTime]
           [java.time.temporal ChronoField]))

(driver/register! :trino-jdbc, :parent #{:presto-jdbc, ::legacy/use-legacy-classes-for-read-and-set})

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Connectivity                                                  |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn- db-name
  "Creates a \"DB name\" for the given catalog `c` and (optional) schema `s`.  If both are specified, a slash is
  used to separate them.  See examples at:
  https://prestodb.io/docs/current/installation/jdbc.html#connecting"
  [c s]
  (cond
    (str/blank? c)
    ""

    (str/blank? s)
    c

    :else
    (str c "/" s)))

(defn- jdbc-spec
  "Creates a spec for `clojure.java.jdbc` to use for connecting to Presto via JDBC, from the given `opts`."
  [{:keys [host port catalog schema]
    :or   {host "localhost", port 5432, catalog ""}
    :as   opts}]
  (-> (merge
       {:classname                     "io.trino.jdbc.TrinoDriver"
        :subprotocol                   "trino"
        :subname                       (db.spec/make-subname host port (db-name catalog schema))}
       (dissoc opts :host :port :db :catalog :schema))
      sql-jdbc.common/handle-additional-options))

(defmethod sql-jdbc.conn/connection-details->spec :trino
  [_ {ssl? :ssl, :as details-map}]
  (let [props (-> details-map
                  (update :port (fn [port]
                                  (if (string? port)
                                    (Integer/parseInt port)
                                    port)))
                  (assoc :SSL ssl?)
                ;; remove any Metabase specific properties that are not recognized by the PrestoDB JDBC driver, which is
                ;; very picky about properties (throwing an error if any are unrecognized)
                ;; all valid properties can be found in the JDBC Driver source here:
                ;; https://github.com/prestodb/presto/blob/master/presto-jdbc/src/main/java/com/facebook/presto/jdbc/ConnectionProperties.java
                  (select-keys [:host :port :catalog :schema :additional-options ; needed for [jdbc-spec]
                              ;; JDBC driver specific properties
                                :user :password :socksProxy :httpProxy :applicationNamePrefix :disableCompression :SSL
                                :SSLKeyStorePath :SSLKeyStorePassword :SSLTrustStorePath :SSLTrustStorePassword
                                :KerberosRemoteServiceName :KerberosPrincipal :KerberosUseCanonicalHostname
                                :KerberosConfigPath :KerberosKeytabPath :KerberosCredentialCachePath :accessToken
                                :extraCredentials :sessionProperties :protocols :queryInterceptors]))]
    (jdbc-spec props)))

(defn- ^TrinoConnection pooled-conn->presto-conn
  "Unwraps the C3P0 `pooled-conn` and returns the underlying `TrinoConnection` it holds."
  [^C3P0ProxyConnection pooled-conn]
  (.unwrap pooled-conn TrinoConnection))

(defn- ^TrinoConnection rs->presto-conn
  "Returns the `TrinoConnection` associated with the given `ResultSet` `rs`."
  [^ResultSet rs]
  (-> (.. rs getStatement getConnection)
      pooled-conn->presto-conn))

;;copied from prestor driver
(defn- ^LocalTime sql-time->local-time
  "Converts the given instance of `java.sql.Time` into a `java.time.LocalTime`, including milliseconds. Needed for
  similar reasons as `set-time-param` above."
  [^Time sql-time]
  ;; Java 11 adds a simpler `ofInstant` method, but since we need to run on JDK 8, we can't use it
  ;; https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/time/LocalTime.html#ofInstant(java.time.Instant,java.time.ZoneId)
  (let [^LocalTime lt (t/local-time sql-time)
        ^Long millis  (mod (.getTime sql-time) 1000)]
    (.with lt ChronoField/MILLI_OF_SECOND millis)))

(defmethod sql-jdbc.execute/connection-with-timezone :trino
  [driver database ^String timezone-id]
  ;; Presto supports setting the session timezone via a `TrinoConnection` instance method. Under the covers,
  ;; this is equivalent to the `X-Presto-Time-Zone` header in the HTTP request (i.e. the `:presto` driver)
  (let [conn            (.getConnection (sql-jdbc.execute/datasource-with-diagnostic-info! driver database))
        underlying-conn (pooled-conn->presto-conn conn)]
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
  (let [zone     (.getTimeZoneId (rs->presto-conn rset))]
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
      (.atOffset instant (t/zone-offset))
      )))

(defmethod sql-jdbc.execute/read-column-thunk [:trino Types/TIME_WITH_TIMEZONE]
  [_ rs _ i]
  ;; (fn []
  ;;   (let [t (.getObject rs i java.time.LocalTime)
  ;;         ;; local-time (t/local-time (.toString t))
  ;;         ;; zone (t/zone-id)
  ;;         ;; zone-rules (.getRules zone)
  ;;         zone-offset (t/zone-offset)
  ;;         zoned-local-time (t/offset-time t zone-offset)]
  ;;     (println (format "SQL TIME: %s" (.toString t)))
  ;;     zoned-local-time)))
  (fn []
    (let [local-time (-> (.getTime rs i)
                         sql-time->local-time)]
      ;; for both `time` and `time with time zone`, the JDBC type reported by the driver is `Types/TIME`, hence
        ;; even though this value is a `LocalTime`, the base-type is time with time zone, so we need to shift it back to
        ;; the UTC (0) offset
       (t/offset-time
         local-time
         (t/zone-offset 0)))))