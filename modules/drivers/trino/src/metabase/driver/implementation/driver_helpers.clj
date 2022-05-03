(ns metabase.driver.implementation.driver-helpers
  "Driver api implementation for Trino JDBC driver."
  (:require [metabase.driver :as driver]
            [metabase.driver.common :as driver.common]))

;;; Trino API helpers

(defmethod driver/db-start-of-week :trino
  [_]
  :monday)

(defmethod driver.common/current-db-time-date-formatters :trino
  [_]
  (driver.common/create-db-time-formatters "yyyy-MM-dd'T'HH:mm:ss.SSSZ"))

(defmethod driver.common/current-db-time-native-query :trino
  [_]
  "select to_iso8601(current_timestamp)")

(defmethod driver/current-db-time :trino
  [& args]
  (apply driver.common/current-db-time args))

(doseq [[feature supported?] {:set-timezone                    true
                              :basic-aggregations              true
                              :standard-deviation-aggregations true
                              :expressions                     true
                              :native-parameters               true
                              :expression-aggregations         true
                              :binning                         true
                              :foreign-keys                    true}]
  (defmethod driver/supports? [:trino feature] [_ _] supported?))