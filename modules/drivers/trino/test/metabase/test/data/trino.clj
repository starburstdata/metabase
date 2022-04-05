(ns metabase.test.data.trino
  "Trino driver test extensions."
  (:require [clojure.string :as str]
            [metabase.config :as config]
            [metabase.connection-pool :as connection-pool]
            [metabase.driver :as driver]
            [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
            [metabase.test.data.interface :as tx]
            [metabase.test.data.sql :as sql.tx]
            [metabase.test.data.sql-jdbc :as sql-jdbc.tx]
            [metabase.test.data.sql-jdbc.execute :as execute]
            [metabase.test.data.sql-jdbc.load-data :as load-data]
            [metabase.test.data.sql.ddl :as ddl]
            [metabase.util :as u])
  (:import [java.sql Connection DriverManager PreparedStatement]))

;; JDBC SQL
(sql-jdbc.tx/add-test-extensions! :trino)