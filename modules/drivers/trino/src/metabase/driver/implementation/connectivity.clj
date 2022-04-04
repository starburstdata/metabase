(ns metabase.driver.implementation.connectivity"Connectivity implementation for Trino JDBC driver."
  (:require [clojure.string :as str]
            [metabase.db.spec :as mdb.spec]
            [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
            [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]))

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
        :subname                       (mdb.spec/make-subname host port (db-name catalog schema))}
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
