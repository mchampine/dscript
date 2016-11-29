(ns dscript.core
  (:require [datascript.core :as d]
            [clojure.string :as string]))

;; exmples from http://udayv.com/clojurescript/clojure/2016/04/28/datascript101/

(def conn (d/create-conn {}))

;; define maker to be a reference
;; define colors to be an array
(def schema {:car/maker {:db/type :db.type/ref}
             :car/colors {:db/cardinality :db.cardinality/many}})

(def conn (d/create-conn schema))

(d/transact! conn [{:maker/name "Honda"
                    :maker/country "Japan"}])
  
(d/transact! conn [{:db/id -1
                    :maker/name "BMW"
                    :maker/country "Germany"}
                   {:car/maker -1
                    :car/name "i525"
                    :car/colors ["red" "green" "blue"]}])

(d/transact! conn [{:db/id -1
                    :maker/name "BMW"
                    :maker/country "Germany"}
                   {:car/maker -1
                    :car/name "i325"
                    :car/colors ["red" "green" "blue"]}])

(d/transact! conn [{:db/id -1
                    :maker/name "Mazda"
                    :maker/country "Japan"}
                   {:car/maker -1
                    :car/name "Miata"
                    :car/colors ["teal" "mauve" "yellow"]}])

(d/q '[:find ?name
       :where
       [?e :maker/name "BMW"]
       [?c :car/maker ?e]
       [?c :car/name ?name]]
     @conn)
; #{["i325"] ["i525"]}

;; or alternatively - but just gets the first one
(let [car-entity (ffirst
                  (d/q '[:find ?c
                         :where
                         [?e :maker/name "BMW"]
                         [?c :car/maker ?e]]
                       @conn))]
  (:car/name (d/entity @conn car-entity)))
; "i525"

;;;; Better

(def schema {:maker/email {:db/unique :db.unique/identity}
             :car/model {:db/unique :db.unique/identity}
             :car/maker {:db/type :db.type/ref}
             :car/colors {:db/cardinality :db.cardinality/many}})

(def conn (d/create-conn schema))

(d/transact! conn [{:maker/email "ceo@bmw.com"
                    :maker/name "BMW"}
                   {:car/model "E39530i"
                    :car/maker [:maker/email "ceo@bmw.com"]
                    :car/name "2003 530i"}])

;; entity lookups
(d/entity @conn [:car/model "E39530i"])       ; {:db/id 2}
(d/entity @conn [:maker/email "ceo@bmw.com"]) ; {:db/id 1}

;; get attribute from entity
(:maker/name (d/entity @conn [:maker/email "ceo@bmw.com"]))
; "BMW"

;; new insert car - need maker
(d/transact! conn [{:car/model "E39520i"
                    :car/maker [:maker/email "ceo@bmw.com"]
                    :car/name "2003 520i"}])

;; lookup all car names for maker
(d/q '[:find [?name ...]
       :where
       [?c :car/maker [:maker/email "ceo@bmw.com"]]
       [?c :car/name ?name]]
     @conn)
; ["2003 530i" "2003 520i"]

;; change maker name
(d/transact! conn [{:maker/email "ceo@bmw.com"
                    :maker/name "BMW Motors"}])

(:maker/name (d/entity @conn [:maker/email "ceo@bmw.com"]))
; "BMW Motors"

