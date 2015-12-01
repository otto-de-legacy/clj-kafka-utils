(defproject de.otto/clj-kafka-utils "1.0.0"
  :description "This library is a collection of utility functions and extensions built on top of clj-kafka"
  :url "https://github.com/otto-de/kafka-utils"
  :license {:name "Apache License 2.0"
            :url  "http://www.apache.org/license/LICENSE-2.0.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.clojure/data.json "0.2.2"]
                 [org.clojure/tools.logging "0.3.0"]
                 [org.apache.kafka/kafka_2.10 "0.8.2.1" :classifier "test"]
                 [clj-kafka "0.3.2"]]
  :test-paths ["test" "test-resources"]
  :plugins [[lein-codox "0.9.0"]]
  :codox {:metadata {:doc/format :markdown}})
