(ns kinesis-to-bigquery.bigquery
  (:require [gclouj.bigquery :as bq]))

(defn insert! [table rows]
  (let [service (bq/service)
        t       (assoc table :row-id bq/row-hash)
        resp    (bq/insert-all service t rows)]
    (when-let [e (:errors resp)]
      (println "error inserting into bigquery:" e))))
