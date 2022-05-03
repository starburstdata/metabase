(ns metabase.driver.implementation.driver-helpers
  "Driver api implementation for Trino JDBC driver."
  (:require [metabase.driver :as driver]))

;;; Trino API helpers

(defmethod driver/db-start-of-week :trino
  [_]
  :monday)

(doseq [[feature supported?] {:set-timezone                    true
                              :basic-aggregations              true
                              :standard-deviation-aggregations true
                              :expressions                     true
                              :native-parameters               true
                              :expression-aggregations         true
                              :binning                         true
                              :foreign-keys                    true}]
  (defmethod driver/supports? [:trino feature] [_ _] supported?))