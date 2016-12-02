(ns dscript.tabular
  (:require [datascript.core :as d]
            [clojure.string :as string]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:require [clojure.pprint :refer [pprint] :rename {pprint pp}]))

;; load CSV data into a DataScript DB and query on it

(def leads 
  (with-open [in-file (io/reader "src/dscript/leads.csv")]
    (doall
     (csv/read-csv in-file))))

; assumes a header row to use as keys
(defn csv-vectors->maps [v]
  (map #(zipmap (map keyword %1)  %2) (repeat (first v)) (rest v)))

;; (with-open [out-file (io/writer "out-file.csv")]
;;   (csv/write-csv out-file
;;                  [["abc" "def"]
;;                   ["ghi" "jkl"]]))

(def conn (d/create-conn {}))

; populate db with csv data
(d/transact! conn (vec (csv-vectors->maps leads)))

;; queries

; contact list
(d/q '[:find ?lead ?phone
       :where
       [?e :Lead ?lead]
       [?e :Phone ?phone]]
     @conn)
; #{["Jim Grayson" "(555)761-2385"] ["Melissa Potter" "(555)791-3471"] ["Prescilla Winston" "(555)218-3981"]}

(defn lead-title-phone
  "Return a lead's title and phone number by name"
  [name]
  (d/q '[:find ?title ?phone
         :in $ ?name
         :where
         [?e :Title ?title]
         [?e :Phone ?phone]
         [?e :Lead ?name]]
       @conn name))

;; use it
(lead-title-phone "Jim Grayson")
; #{["Senior Manager" "(555)761-2385"]}

(lead-title-phone "Prescilla Winston")
; #{["Development Director" "(555)218-3981"]}












