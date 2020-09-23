(ns datahike.test.query-find-specs
  (:require
    #?(:cljs [cljs.test :as t :refer-macros [is deftest testing]]
       :clj  [clojure.test :as t :refer [is deftest testing]])
    [datahike.test.core :as tdc]
    [datahike.core :as d]))


(def test-db (d/db-with
               (d/empty-db)
               [[:db/add tdc/e1 :name "Petr"]
                [:db/add tdc/e1 :age 44]
                [:db/add tdc/e2 :name "Ivan"]
                [:db/add tdc/e2 :age 25]
                [:db/add tdc/e3 :name "Sergey"]
                [:db/add tdc/e3 :age 11]]))

(deftest test-find-specs
  (is (= (set (d/q '[:find [?name ...]
                     :where [_ :name ?name]] test-db))
         #{"Ivan" "Petr" "Sergey"}))
  (is (= (d/q '[:find [?name ?age]
                :where [tdc/e1 :name ?name]
                [tdc/e1 :age ?age]] test-db)
         ["Petr" 44]))
  (is (= (d/q '[:find ?name .
                :where [tdc/e1 :name ?name]] test-db)
         "Petr"))

  (testing "Multiple results get cut"
    (is (contains?
          #{["Petr" 44] ["Ivan" 25] ["Sergey" 11]}
          (d/q '[:find [?name ?age]
                 :where [?e :name ?name]
                 [?e :age ?age]] test-db)))
    (is (contains?
          #{"Ivan" "Petr" "Sergey"}
          (d/q '[:find ?name .
                 :where [_ :name ?name]] test-db))))

  (testing "Aggregates work with find specs"
    (is (= (d/q '[:find [(count ?name) ...]
                  :where [_ :name ?name]] test-db)
           [3]))
    (is (= (d/q '[:find [(count ?name)]
                  :where [_ :name ?name]] test-db)
           [3]))
    (is (= (d/q '[:find (count ?name) .
                  :where [_ :name ?name]] test-db)
           3)))
  )

#_(t/test-ns 'datahike.test.query-find-specs)