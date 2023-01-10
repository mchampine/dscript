(ns dscript.tabular
  (:require [datascript.core :as d]
            [clojure.string :as string]
            [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:require [clojure.pprint :refer [pprint] :rename {pprint pp}]))

;; load CSV data into a DataScript DB and query on it
;; This exercise inspired by an Eric Normand Tutorial using SQL to do store/query CSV data.

(def leads 
  (with-open [in-file (io/reader "src/dscript/leads.csv")]
    (doall
     (csv/read-csv in-file))))

; assumes a header row to use as keys
(defn csv-vectors->maps [v]
  (map #(zipmap (map keyword %1)  %2) (repeat (first v)) (rest v)))

(def conn (d/create-conn {})) ; no schema

; populate db with csv data
(d/transact! conn (vec (csv-vectors->maps leads)))

;;;; queries

;; contact list
(d/q '[:find ?lead ?phone
       :where
       [?e :Lead ?lead]
       [?e :Phone ?phone]]
     @conn)
; #{["Jim Grayson" "(555)761-2385"] ["Melissa Potter" "(555)791-3471"] ["Prescilla Winston" "(555)218-3981"]}

;; how many entities?
(d/q '[:find (count ?e)
       :where
       [?e :Lead]]
     @conn)
; ([3])

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

;; fetch some attrs by entity
(d/pull @conn [:Lead :Phone] 1)
; {:Lead "Jim Grayson", :Phone "(555)761-2385"}

;; fetch pattern match attrs by entity
(d/pull @conn '[*] 2)
; {:db/id 2, :Lead "Prescilla Winston", :Notes "said to call again next week", :Phone "(555)218-3981", :Title "Development Director"}
