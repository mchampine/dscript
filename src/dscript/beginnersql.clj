(ns dscript.beginnersql
  (:require [datascript.core :as d]
            [clojure.string :as string]))

;; SQL example translated to DataScript

;; This is a translation of the samples in "A BEGINNERS GUIDE TO SQL"
;; http://www.sohamkamani.com/blog/2016/07/07/a-beginners-guide-to-sql/
;; Into DataScript schema, data, and queries

(def bookschema
  {:member/id {:db.unique :db.unique/identity}
   :book/id {:db.unique :db.unique/identity}
   :book/title {}
   :book/author {}
   :borrowings/bookid {:db/type :db.type/ref}
   :borrowings/memberid {:db/type :db.type/ref}})

(def bookconn (d/create-conn bookschema))

(d/transact! bookconn [
                       {:member/id 1
                        :member/firstname "Sue"
                        :member/lastname "Mason"}
                       {:member/id 2
                        :member/firstname "Ellen"
                        :member/lastname "Horton"}
                       {:member/id 3
                        :member/firstname "Henry"
                        :member/lastname "Clarke"}
                       {:member/id 4
                        :member/firstname "Mike"
                        :member/lastname "Willis"}
                       {:member/id 5
                        :member/firstname "Lida"
                        :member/lastname "Tyler"}])

(d/transact! bookconn [
                       {:book/id 1
                        :book/title "Scion of Ikshvaku"
                        :book/author "Amish Tripathi"
                        :book/published "06-22-2015"
                        :book/stock 2}
                       {:book/id 2
                        :book/title "The Lost Symbol"
                        :book/author "Dan Brown"
                        :book/published "07-22-2010"
                        :book/stock 3}
                       {:book/id 3
                        :book/title "Who Will Cry When You Die?"
                        :book/author "Robin Sharma"
                        :book/published "06-15-2006"
                        :book/stock 4}
                       {:book/id 4
                        :book/title "Inferno"
                        :book/author "Dan Brown"
                        :book/published "05-05-2014"
                        :book/stock 2}
                       {:book/id 5
                        :book/title "The Fault in our Stars"
                        :book/author "John Green"
                        :book/published "01-03-2015"
                        :book/stock 3}
                        ])

(d/transact! bookconn [
                       {:borrowings/bookid [:book/id 1]
                        :borrowings/memberid [:member/id 3]
                        :borrowings/borrowdate "01-20-2016"
                        :borrowings/returndate "03-17-2016"}
                       {:borrowings/bookid [:book/id 2]
                        :borrowings/memberid [:member/id 4]
                        :borrowings/borrowdate "01-19-2016"
                        :borrowings/returndate "03-23-2016"}
                       {:borrowings/bookid [:book/id 1]
                        :borrowings/memberid [:member/id 1]
                        :borrowings/borrowdate "02-17-2016"
                        :borrowings/returndate "05-18-2016"}
                       {:borrowings/bookid [:book/id 4]
                        :borrowings/memberid [:member/id 2]
                        :borrowings/borrowdate "12-15-2016"
                        :borrowings/returndate "04-13-2016"}
                       {:borrowings/bookid [:book/id 2]
                        :borrowings/memberid [:member/id 2]
                        :borrowings/borrowdate "01-18-2016"
                        :borrowings/returndate "04-19-2016"}
                       {:borrowings/bookid [:book/id 3]
                        :borrowings/memberid [:member/id 5]
                        :borrowings/borrowdate "02-29-2016"
                        :borrowings/returndate "04-11-2016"}
                        ])

;; entity ids of all books
(d/q '[:find ?e
       :where [?e :book/author]]
     @bookconn)

;;;;;; QUERIES ;;;;;;

;; names and ids of dan brown books
(d/q '[:find ?id ?title
       :where
       [?a :book/author "Dan Brown"]
       [?a :book/title ?title]
       [?a :book/id ?id]]
     @bookconn)
;; ==> #{[2 "The Lost Symbol"] [4 "Inferno"]}

;; all borrowed books written by dan brown w/ return date
(d/q '[:find ?title ?return
       :where
       [?bkid :book/author "Dan Brown"]
       [?bbkid :borrowings/bookid ?bkid]
       [?bkid :book/title ?title]
       [?bbkid :borrowings/returndate ?return]]
     @bookconn)
;; ==> #{["Inferno" "04-13-2016"] ["The Lost Symbol" "04-19-2016"] ["The Lost Symbol" "03-23-2016"]}

;; first and last name of everyone who has borrowed a book by dan brown
(d/q '[:find ?first ?last
       :where
       [?bkid :book/author "Dan Brown"]
       [?bbkid :borrowings/bookid ?bkid]
       [?bbkid :borrowings/memberid ?memid]
       [?memid :member/firstname ?first]
       [?memid :member/lastname ?last]]
     @bookconn)
;; ==> #{["Mike" "Willis"] ["Ellen" "Horton"]}

;; number of dan brown books borrowed per member
(d/q '[:find ?first ?last (count ?bkid)
       :where
       [?bkid :book/author "Dan Brown"]
       [?bbkid :borrowings/bookid ?bkid]
       [?bbkid :borrowings/memberid ?memid]
       [?memid :member/firstname ?first]
       [?memid :member/lastname ?last]]
     @bookconn)
;; ==> (["Ellen" "Horton" 2] ["Mike" "Willis" 1])

;; total stock of books per author
;; BUG!  Returns 3 when both dan brown books stock is 3
;; for now changed one of of the stock values
(d/q '[:find ?author (sum ?stock)
       :where
       [?b :book/author ?author]
       [?b :book/stock ?stock]]
     @bookconn)
;; ==> (["Amish Tripathi" 2] ["John Green" 3] ["Robin Sharma" 4] ["Dan Brown" 5])

;; stock of books by a given author
(d/q '[:find ?author (sum ?stock)
       :where
       [?b :book/author "Robin Sharma"]
       [?b :book/author ?author]
       [?b :book/stock ?stock]
       ]
     @bookconn)
;; ==> (["Robin Sharma" 4])

;; title and ID of books written by authors whose stock > 3
(d/q '[:find ?title ?id
       :in $ [[?author ?totalstock]]
       :where
       [?a :book/author ?author]
       [?a :book/title ?title]
       [?a :book/id ?id]
       [(> ?totalstock 3)]]
     @bookconn
     (d/q '[:find ?author (sum ?stock)
            :where
            [?b :book/author ?author]
            [?b :book/stock ?stock]]
          @bookconn))
;; #{["Who Will Cry When You Die?" 3] ["The Lost Symbol" 2] ["Inferno" 4]}

;; Books that have above average stock
(d/q '[:find ?title
       :in $ [[?avgstock]]
       :where
       [?a :book/stock ?s]
       [?a :book/title ?title]
       [(> ?s ?avgstock)]]
     @bookconn
     (d/q '[:find (avg ?stock)
            :where
            [_ :book/stock ?stock]]
          @bookconn))
; #{["Who Will Cry When You Die?"]}

;; update Dan Brown stock to zero

; get dan brown entity ids and stock per title
(d/q '[:find ?e ?title ?stock
       :where
       [?e :book/author "Dan Brown"]
       [?e :book/title ?title]
       [?e :book/stock ?stock]]
     @bookconn)
; #{[9 "Inferno" 2] [7 "The Lost Symbol" 3]}
; note, entity ids may change if book is deleted and recreated

; look up dan brown book entities and set stock to 0 in each
(let [eids (d/q '[:find ?e
                  :where
                  [?e :book/author "Dan Brown"]]
                @bookconn)
      tx (fn [eid]
           (d/transact! bookconn [{:db/id (first eid) :book/stock 0}]))]
  (map tx eids))

;; restore stock lost symbol = 3, inferno = 2
(let [inferno (d/q '[:find ?e
                  :where
                  [?e :book/title "Inferno"]]
                   @bookconn)
      symbol (d/q '[:find ?e
                  :where
                  [?e :book/title "The Lost Symbol"]]
                   @bookconn)]
  (d/transact! bookconn [{:db/id (ffirst inferno) :book/stock 2}
                         {:db/id (ffirst symbol) :book/stock 3}]))

;; Delete Dan Brown books
(let [eids (d/q '[:find ?e
                  :where
                  [?e :book/author "Dan Brown"]]
                @bookconn)
      tx (fn [eid]
           (d/transact! bookconn [[:db.fn/retractEntity (first eid)]]))]
  (map tx eids))

;; members who borrowed any book with a total stock that was above average.
(d/q '[:find ?first ?last
       :in $ [[?avgstock]]
       :where
       [?a :book/stock ?s]
       [(> ?s ?avgstock)]
       [?b :borrowings/bookid ?a]     ;book entity => borrowings member
       [?b :borrowings/memberid ?m]
       [?n :member/id ?m]             ;borrowings member => member
       [?n :member/firstname ?first]
       [?n :member/lastname ?last]]
     @bookconn
     (d/q '[:find (avg ?stock)
            :where
            [_ :book/stock ?stock]]
          @bookconn))
; #{["Lida" "Tyler"]}
