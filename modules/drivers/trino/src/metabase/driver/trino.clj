(ns metabase.driver.trino
  "Trino JDBC driver."
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [metabase.driver :as driver]
            [java-time :as t]
            [metabase.util :as u]
            [metabase.util.date-2 :as u.date]
            [metabase.driver.presto-jdbc :as presto-jdbc]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.db.spec :as db.spec]
            [metabase.driver.sql-jdbc.execute.legacy-impl :as legacy]
            [metabase.util.i18n :refer [trs]])
  
  (:import io.trino.jdbc.TrinoConnection
          [java.sql Connection PreparedStatement ResultSet ResultSetMetaData Time Types]
          [java.time LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]
          com.mchange.v2.c3p0.C3P0ProxyConnection))

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
  (println "Custom Thunk Types/TIMESTAMP...")
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
            (-> (t/zoned-date-time t zone)
                (u.date/with-time-zone-same-instant "UTC")
                t/local-date-time)
            :else
            t))))))

(defmethod sql-jdbc.execute/read-column-thunk [:trino Types/TIMESTAMP_WITH_TIMEZONE]
  [_ ^ResultSet rset _ ^long i]
  (println "Custom Thunk Types/TIMESTAMP_WITH_TIMEZONE...")
  (fn []
    (let [t (.getObject rset i java.sql.Timestamp)
          instant (.toInstant t)]
      (.atOffset instant (t/zone-offset))
      )))

(defmethod sql-jdbc.execute/read-column-thunk [:trino Types/TIME_WITH_TIMEZONE]
  [_ rs _ i]
  (println "Custom Thunk TIME_WITH_TIMEZONE...")
  (fn []
    (let [t (.getObject rs i java.sql.Time)
          local-time (t/local-time (.toString t))
          ;; zone (t/zone-id)
          ;; zone-rules (.getRules zone)
          zone-offset (t/zone-offset)
          zoned-local-time (t/offset-time local-time zone-offset)]
      (println (format "SQL TIME: %s" (.toString t)))
      zoned-local-time)))