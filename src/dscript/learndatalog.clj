(ns dscript.learndatalog
  (:require [datascript.core :as d]
            [clojure.string :as string]))


;; DataScript translation of examples and exercieses from Learn Datalog Today
;; See http://www.learndatalogtoday.org/

;; schema and data from https://gist.github.com/fasiha/2ab2c1cb203c26a2b63532831f1b6021
;; see that url for schema.edn and data.edn files and import/conversion functions

(def schema
  {:movie/title {:db/cardinality :db.cardinality/one},
   :movie/year {:db/cardinality :db.cardinality/one},
   :movie/director
   {:db/valueType :db.type/ref, :db/cardinality :db.cardinality/many},
   :movie/sequel
   {:db/valueType :db.type/ref, :db/cardinality :db.cardinality/one},
   :movie/cast
   {:db/valueType :db.type/ref, :db/cardinality :db.cardinality/many},
   :person/name {:db/cardinality :db.cardinality/one},
   :person/born {:db/cardinality :db.cardinality/one},
   :person/death {:db/cardinality :db.cardinality/one},
   :trivia {:db/cardinality :db.cardinality/many}})

;; data ingest from file
(def data (->> "src/dscript/data.txt" slurp read-string))

; create a DataScript db using the schema and dump the data into it
(def conn (d/create-conn schema))
(d/transact! conn data)

; try a query
(d/q '[:find ?title
       :where
       [_ :movie/title ?title]]
     @conn)
; #{["First Blood"] ["Terminator 2: Judgment Day"] ["The Terminator"] . . .


;;;;;;;;;;;;;;;;;;;;;  learndatalogtoday.org  ;;;;;;;;;;;;;;;;;;;;;;

;;;;; chapter 1 basic queries

;; example
;; find all entity-ids that have the attribute :person/name with a value of "Ridley Scott"
(d/q '[:find ?e
       :where
       [?e :person/name "Ridley Scott"]]
     @conn)
; #{[38]}

;; exercises

;; Find the entity ids of movies made in 1987
(d/q '[:find ?e
       :where
       [?e :movie/year 1987]]
     @conn)
; #{[59] [55] [57]}

;; Find the entity-id and titles of movies in the database
(d/q '[:find ?e ?title
       :where
       [?e :movie/title ?title]]
     @conn)
; #{[66 "Aliens"] [67 "Mad Max"] [53 "First Blood"] [58 "Lethal Weapon 2"] [65 "Alien"] [54 "Rambo: First Blood Part II"] [59 "RoboCop"] [60 "Commando"] [63 "Rambo III"] [52 "Terminator 2: Judgment Day"] [56 "Predator 2"] [70 "Braveheart"] [69 "Mad Max Beyond Thunderdome"] [68 "Mad Max 2"] [62 "Terminator 3: Rise of the Machines"] [57 "Lethal Weapon"] [51 "The Terminator"] [61 "Die Hard"] [64 "Lethal Weapon 3"] [55 "Predator"]}

;; Find the name of all people in the database
(d/q '[:find ?name
       :where
       [?p :person/name ?name]]
     @conn)
; #{["Rae Dawn Chong"] ["Joe Pesci"] ["Brian Dennehy"] ["Nick Stahl"] ["Carrie Henn"] ["Tom Skerritt"] ["George P. Cosmatos"] ["Paul Verhoeven"] ["Alan Rickman"] ["Peter MacDonald"] ["Alexander Godunov"] ["Bruce Willis"] ["Tina Turner"] ["Claire Danes"] ["Danny Glover"] ["Mark L. Lester"] ["Ridley Scott"] ["Peter Weller"] ["Bruce Spence"] ["Michael Preston"] ["Jonathan Mostow"] ["Ruben Blades"] ["Sigourney Weaver"] ["Joanne Samuel"] ["Stephen Hopkins"] ["Michael Biehn"] ["George Ogilvie"] ["Ted Kotcheff"] ["Steve Bisley"] ["Charles Napier"] ["Carl Weathers"] ["Robert Patrick"] ["John McTiernan"] ["Richard Donner"] ["Marc de Jonge"] ["Gary Busey"] ["Sylvester Stallone"] ["Nancy Allen"] ["Mel Gibson"] ["Elpidia Carrillo"] ["Ronny Cox"] ["Veronica Cartwright"] ["Edward Furlong"] ["Richard Crenna"] ["Arnold Schwarzenegger"] ["James Cameron"] ["Alyssa Milano"] ["Sophie Marceau"] ["George Miller"] ["Linda Hamilton"]}

;;;;; Chapter 2 - Data patterns

;; example: who starred in "Lethal Weapon"?
(d/q '[:find ?name
       :where
       [?m :movie/title "Lethal Weapon"]
       [?m :movie/cast ?p]
       [?p :person/name ?name]]
     @conn)
; #{["Danny Glover"] ["Gary Busey"] ["Mel Gibson"]}

;; exercises

; Find movie titles made in 1985
(d/q '[:find ?title
       :where
       [?m :movie/title ?title]
       [?m :movie/year 1985]]
     @conn)
; #{["Rambo: First Blood Part II"] ["Commando"] ["Mad Max Beyond Thunderdome"]}

; What year was "Alien" released?
(d/q '[:find ?year
       :where
       [?m :movie/title "Alien"]
       [?m :movie/year ?year]]
     @conn)
; #{[1979]}

; Who directed RoboCop?
(d/q '[:find ?name
       :where
       [?m :movie/title "RoboCop"]
       [?m :movie/director ?d]
       [?d :person/name ?name]]
     @conn)
; #{["Paul Verhoeven"]}

; Find directors who have directed Arnold Schwarzenegger in a movie.
(d/q '[:find ?name
 :where
       [?p :person/name "Arnold Schwarzenegger"]
       [?m :movie/cast ?p]
       [?m :movie/director ?d]
       [?d :person/name ?name]]
     @conn)
; #{["Mark L. Lester"] ["Jonathan Mostow"] ["John McTiernan"] ["James Cameron"]}


;;;; Chapter 3 - Parameterized queries

;; example - find movies for a given actor
(d/q '[:find ?title
       :in $ ?name
       :where
       [?p :person/name ?name]
       [?m :movie/cast ?p]
       [?m :movie/title ?title]]
     @conn
     "Sylvester Stallone")
; #{["First Blood"] ["Rambo III"] ["Rambo: First Blood Part II"]}

;; tuples example - cameron swartzenegger collaboration w/ destructuring
(d/q '[:find ?title
       :in $ [?director ?actor]
       :where
       [?d :person/name ?director]
       [?a :person/name ?actor]
       [?m :movie/director ?d]
       [?m :movie/cast ?a]
       [?m :movie/title ?title]]
     @conn ["James Cameron" "Arnold Schwarzenegger"])
; #{["Terminator 2: Judgment Day"] ["The Terminator"]} 

;; collections example - cameron or scott movies
(d/q '[:find ?title
       :in $ [?director ...]
       :where
       [?p :person/name ?director]
       [?m :movie/director ?p]
       [?m :movie/title ?title]]
     @conn ["James Cameron" "Ridley Scott"])
; #{["Terminator 2: Judgment Day"] ["The Terminator"] ["Alien"] ["Aliens"]}

;; relations example - box office per director
(d/q '[:find ?title ?box-office
       :in $ ?director [[?title ?box-office]]
       :where
       [?p :person/name ?director]
       [?m :movie/director ?p]
       [?m :movie/title ?title]]
     @conn "John McTiernan"
     [["Die Hard" 140700000]
      ["Alien" 104931801]
      ["Lethal Weapon" 120207127]
      ["Commando" 57491000]])
; #{["Die Hard" 140700000]}

;; exercises 

;; movie title by year for 1988
(d/q '[:find ?title
       :in $ ?year
       :where
       [?m :movie/title ?title]
       [?m :movie/year ?year]]
     @conn 1988)
; #{["Rambo III"] ["Die Hard"]}

;; Given a list of movie titles, find the title and the year that movie was released.
(d/q '[:find ?title ?year
       :in $ [?title ...]
       :where
       [?m :movie/title ?title]
       [?m :movie/year ?year]]
     @conn ["Lethal Weapon" "Lethal Weapon 2" "Lethal Weapon 3"])
; #{["Lethal Weapon" 1987] ["Lethal Weapon 2" 1989] ["Lethal Weapon 3" 1992]}

;; Find all movie ?titles where the ?actor and the ?director have worked together
(d/q '[:find ?title
       :in $ ?actor ?director
       :where
       [?a :person/name ?actor]
       [?m :movie/director ?d]
       [?m :movie/cast ?a]
       [?m :movie/title ?title]]
     @conn "Michael Biehn" "James Cameron")
; #{["The Terminator"] ["Aliens"]}

;; Write a query that, given an actor name and a relation with
;; movie-title/rating, finds the movie titles and corresponding rating
;; for which that actor was a cast member.
(d/q '[:find ?title ?rating
       :in $ ?actor [[?title ?rating]]
       :where
       [?a :person/name ?actor]
       [?m :movie/cast ?a]
       [?m :movie/title ?title]]
     @conn "Mel Gibson"
     [["Die Hard" 8.3]
      ["Alien" 8.5]
      ["Lethal Weapon" 7.6]
      ["Commando" 6.5]
      ["Mad Max Beyond Thunderdome" 6.1]
      ["Mad Max 2" 7.6]
      ["Rambo: First Blood Part II" 6.2]
      ["Braveheart" 8.4]
      ["Terminator 2: Judgment Day" 8.6]
      ["Predator 2" 6.1]
      ["First Blood" 7.6]
      ["Aliens" 8.5]
      ["Terminator 3: Rise of the Machines" 6.4]
      ["Rambo III" 5.4]
      ["Mad Max" 7.0]
      ["The Terminator" 8.1]
      ["Lethal Weapon 2" 7.1]
      ["Predator" 7.8]
      ["Lethal Weapon 3" 6.6]
      ["RoboCop" 7.5]])
; #{["Mad Max" 7.0] ["Mad Max 2" 7.6] ["Lethal Weapon" 7.6] ["Lethal Weapon 3" 6.6] ["Lethal Weapon 2" 7.1] ["Braveheart" 8.4] ["Mad Max Beyond Thunderdome" 6.1]}


;;;; Chapter 4 - More Queries

;; example - other attributes of person
(d/q '[:find ?attr
       :where 
       [?p :person/name]
       [?p ?attr]]
     @conn)
; #{[:person/name] [:person/born] [:person/death]}

;; example - keywords from above attrs
;; Change required for DataScript compatibility
(d/q '[:find ?attr
       :where
       [?p :person/name]
       [?p ?attr]]  ; not [?p ?a] [?a :db/ident ?attr] because DS has no :db/ident 
     @conn)
; #{[:person/name] [:person/born] [:person/death]}

;; find the time that "James Cameron" was set as the name for that person entity:
;; Note: DataScript doesn't add timestamps so this returns empty set
(d/q '[:find ?timestamp
       :where
       [?p :person/name "James Cameron" ?tx]
       [?tx :db/txInstant ?timestamp]]
     @conn)
; #{}

;; exercises

;; What attributes are associated with a given movie?
;; Change required for DataScript compatibility
(d/q '[:find ?attr
       :in $ ?title
       :where
       [?m :movie/title ?title]
       [?m ?attr]]    ; not [?m ?a] [?a :db/ident ?attr] becase DS has no :db/ident
     @conn "Commando")
; 

;; Find the names of all people associated with a particular movie
(d/q '[:find ?name
       :in $ ?title [?attr ...]
       :where
       [?m :movie/title ?title]
       [?m ?attr ?p]
       [?p :person/name ?name]]
     @conn "Die Hard" [:movie/cast :movie/director])
; #{["Alan Rickman"] ["Alexander Godunov"] ["Bruce Willis"] ["John McTiernan"]}

;; Find all available attributes, their type and their cardinality. (dump schema)
;; Schema is not queryable in DataScript so this returns empty set
(d/q '[:find ?attr ?type ?card
       :where
       [_ :db.install/attribute ?a]
       [?a :db/valueType ?t]
       [?a :db/cardinality ?c]
       [?a :db/ident ?attr]
       [?t :db/ident ?type]
       [?c :db/ident ?card]]
     @conn)
; #{}

;; When was the seed data imported into the database?
;; Note: DataScript doesn't add timestamps so this returns empty set
(d/q '[:find ?inst
       :where
       [_ :movie/title _ ?tx]
       [?tx :db/txInstant ?inst]]
     @conn)
; #{}

;;;; Chapter 5 - Predicates

;; example - Find all movies released before 1984
(d/q '[:find ?title
       :where
       [?m :movie/title ?title]
       [?m :movie/year ?year]
       [(< ?year 1984)]]
     @conn)
; #{["First Blood"] ["Alien"] ["Mad Max 2"] ["Mad Max"]}

;; Any Clojure function as predicate
;; Change required for DataScript compatibility
(d/q '[:find ?name
       :where 
       [?p :person/name ?name]
       [(clojure.string/starts-with? ?name "M")]]  ; instead of (.startsWith ?name "M")
     @conn)
; #{["Mark L. Lester"] ["Michael Preston"] ["Michael Biehn"] ["Marc de Jonge"] ["Mel Gibson"]}

;; exercises

;; movies older than a given year (inclusive)
(d/q '[:find ?title
       :in $ ?olderthan
       :where
       [?m :movie/title ?title]
       [?m :movie/year ?year]
       [(<= ?year ?olderthan)]]
     @conn 1979)
; #{["Alien"] ["Mad Max"]}

;; actors older than danny glover
;; Change required for DataScript compatibility
(defn <time [d1 d2] (.before d1 d2)) ; util for < comparison for time

(d/q '[:find ?actor
       :where
       [?d :person/name "Danny Glover"]
       [?d :person/born ?b1]
       [?e :person/born ?b2]
       [_ :movie/cast ?e]
       [(dscript.learndatalog/<time ?b2 ?b1)] ; instead of "<"
       [?e :person/name ?actor]]
     @conn)
; #{["Joe Pesci"] ["Brian Dennehy"] ["Tom Skerritt"] ["Alan Rickman"] ["Tina Turner"] ["Bruce Spence"] ["Michael Preston"] ["Charles Napier"] ["Gary Busey"] ["Sylvester Stallone"] ["Ronny Cox"] ["Richard Crenna"]}

;; Find movies newer than ?year (inclusive) and has a ?rating higher than the one supplied
(d/q '[:find ?title
       :in $ ?year ?supplied-rating [[?title ?rating]]
       :where
       [(< ?supplied-rating ?rating)]
       [?m :movie/title ?title]
       [?m :movie/year ?y]
       [(<= ?year ?y)]]
     @conn 1990 8.0
     [["Die Hard" 8.3]
      ["Alien" 8.5]
      ["Lethal Weapon" 7.6]
      ["Commando" 6.5]
      ["Mad Max Beyond Thunderdome" 6.1]
      ["Mad Max 2" 7.6]
      ["Rambo: First Blood Part II" 6.2]
      ["Braveheart" 8.4]
      ["Terminator 2: Judgment Day" 8.6]
      ["Predator 2" 6.1]
      ["First Blood" 7.6]
      ["Aliens" 8.5]
      ["Terminator 3: Rise of the Machines" 6.4]
      ["Rambo III" 5.4]
      ["Mad Max" 7.0]
      ["The Terminator" 8.1]
      ["Lethal Weapon 2" 7.1]
      ["Predator" 7.8]
      ["Lethal Weapon 3" 6.6]
      ["RoboCop" 7.5]])
; #{["Terminator 2: Judgment Day"] ["Braveheart"]}

;;; Chapter 6 - Transformation functions

;; example - calculate the age of a person inside a query
;; Change required for DataScript compatibility
(defn age [birthday today]
  (quot (- (.getTime today)
           (.getTime birthday))
        (* 1000 60 60 24 365)))

(d/q '[:find ?age
       :in $ ?name ?today
       :where
       [?p :person/name ?name]
       [?p :person/born ?born]
       [(dscript.learndatalog/age ?born ?today) ?age]] ; full path to age function
     @conn "Gary Busey" #inst "2016-11-27T00:00:00.000-00:00")
; #{[72]}

;; exercises

;; Find people by age.
;; Change required for DataScript compatibility
(d/q '[:find ?name ?age
       :in $ ?a ?today
       :where
       [?p :person/name ?name]
       [?p :person/born ?born]
       [(dscript.learndatalog/age ?born ?today) ?age] ; full path to age function
       [(= ?age ?a)]]
     @conn 63 #inst "2013-08-02T00:00:00.000-00:00")
; #{["Sigourney Weaver" 63] ["Alexander Godunov" 63] ["Nancy Allen" 63]}

;; Find people younger than Bruce Willis and their ages
;; Change required for DataScript compatibility
(d/q '[:find ?name ?age
       :in $ ?today
       :where
       [?p :person/name "Bruce Willis"]
       [?p :person/born ?sborn]
       [?p2 :person/name ?name]
       [?p2 :person/born ?born]
       [(dscript.learndatalog/<time ?sborn ?born)]     ; instead of "<"
       [(dscript.learndatalog/age ?born ?today) ?age]] ; full path required
     @conn #inst "2013-08-02T00:00:00.000-00:00")
; #{["Mel Gibson" 57] ["Michael Biehn" 57] ["Nick Stahl" 33] ["Jonathan Mostow" 51] ["Edward Furlong" 36] ["Linda Hamilton" 56] ["Sophie Marceau" 46] ["Elpidia Carrillo" 51] ["Claire Danes" 34] ["Rae Dawn Chong" 52] ["Alyssa Milano" 40] ["Robert Patrick" 54]}

;; who has the same birthday?
;; Change required for DataScript compatibility
(defn datemonth [datestamp]
  [(.getDate datestamp) (.getMonth datestamp)])

(defn str< [s1 s2] ; < for strings
  (< (compare s1 s2) 0))

(d/q '[:find ?name-1 ?name-2
       :where
       [?p :person/name ?name-1]
       [?p :person/born ?p1born]
       [?p2 :person/name ?name-2]
       [?p2 :person/born ?p2born]
       [(dscript.learndatalog/datemonth ?p1born) ?p1bday]  ; special date month function
       [(dscript.learndatalog/datemonth ?p2born) ?p1bday]  
       [(dscript.learndatalog/str< ?name-1 ?name-2)]]      ; special str comparison
     @conn)
; #{["Elpidia Carrillo" "James Cameron"] ["Alexander Godunov" "Jonathan Mostow"] ["Mark L. Lester" "Tina Turner"] ["Charles Napier" "Claire Danes"] ["Richard Crenna" "Ridley Scott"] ["Nancy Allen" "Peter Weller"]}


;;;; Chapter 7 - Aggregates

;; exercises

;; count the number of movies in the database
(d/q '[:find (count ?m)
       :where
       [?m :movie/title]]
     @conn)
; ([20])

;; Find the birth date of the oldest person in the database.
(d/q '[:find (min ?date)
       :where
       [_ :person/born ?date]]
     @conn)
; ([#inst "1926-11-30T00:00:00.000-00:00"])

;; Find the average rating for each actor.
(d/q '[:find ?name (avg ?rating)
       :in $ [?name ...] [[?title ?rating]]
       :where
       [?p :person/name ?name]
       [?m :movie/cast ?p]
       [?m :movie/title ?title]]
     @conn
     ["Sylvester Stallone" "Arnold Schwarzenegger" "Mel Gibson"]
     [["Die Hard" 8.3]
      ["Alien" 8.5]
      ["Lethal Weapon" 7.6]
      ["Commando" 6.5]
      ["Mad Max Beyond Thunderdome" 6.1]
      ["Mad Max 2" 7.6]
      ["Rambo: First Blood Part II" 6.2]
      ["Braveheart" 8.4]
      ["Terminator 2: Judgment Day" 8.6]
      ["Predator 2" 6.1]
      ["First Blood" 7.6]
      ["Aliens" 8.5]
      ["Terminator 3: Rise of the Machines" 6.4]
      ["Rambo III" 5.4]
      ["Mad Max" 7.0]
      ["The Terminator" 8.1]
      ["Lethal Weapon 2" 7.1]
      ["Predator" 7.8]
      ["Lethal Weapon 3" 6.6]
      ["RoboCop" 7.5]])
; (["Sylvester Stallone" 6.3999999999999995] ["Arnold Schwarzenegger" 7.4799999999999995] ["Mel Gibson" 7.133333333333334])


;;;; Chapter 8 - Rules

;; find moves for year using a rule
(d/q '[:find ?title
       :in $ %
       :where
       [movie-year ?title 1991]]
     @conn
     '[[(movie-year ?title ?year) ; define rule "movie-year"
       [?m :movie/title ?title]
       [?m :movie/year ?year]]])
; #{["Terminator 2: Judgment Day"]}

;; find people who have worked together using "friends" rule
(d/q '[:find ?friend
       :in $ % ?name
       :where
       [?p1 :person/name ?name]
       (friends ?p1 ?p2)
       [?p2 :person/name ?friend]]
     @conn
     '[[(friends ?p1 ?p2)    ; define rule "friends"
        [?m :movie/cast ?p1]
        [?m :movie/cast ?p2]
        [(not= ?p1 ?p2)]]
       [(friends ?p1 ?p2)
        [?m :movie/cast ?p1]
        [?m :movie/director ?p2]]
       [(friends ?p1 ?p2)
        (friends ?p2 ?p1)]]
     "Sigourney Weaver")
; #{["Carrie Henn"] ["Tom Skerritt"] ["Ridley Scott"] ["Michael Biehn"] ["Veronica Cartwright"] ["James Cameron"]}

;; find sequels
(d/q '[:find ?sequel
       :in $ % ?title
       :where
       [?m :movie/title ?title]
       (sequels ?m ?s)
       [?s :movie/title ?sequel]]
     @conn
     '[[(sequels ?m1 ?m2)
        [?m1 :movie/sequel ?m2]]
       [(sequels ?m1 ?m2)
        [?m :movie/sequel ?m2]
        (sequels ?m1 ?m)]]
     "Mad Max")
;; #{["Mad Max 2"] ["Mad Max Beyond Thunderdome"]}
