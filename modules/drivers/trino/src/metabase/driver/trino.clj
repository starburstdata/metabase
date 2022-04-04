(ns metabase.driver.trino
  "Trino JDBC driver."
  (:require [metabase.driver :as driver]))

(prefer-method driver/supports? [:trino :set-timezone] [:sql-jdbc :set-timezone])

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                  Load implemetation files                                      |
;;; +----------------------------------------------------------------------------------------------------------------+
(load "implementation/query_processor")
(load "implementation/sync")
(load "implementation/execute")
(load "implementation/connectivity")
(load "implementation/unprepare")
(load "implementation/driver_helpers")