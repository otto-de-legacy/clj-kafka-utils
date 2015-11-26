(ns de.otto.clj-kafka-utils.core-test
  (:require [clojure.test :refer :all]
            [de.otto.clj-kafka-utils.test-helpers :refer :all]
            [de.otto.clj-kafka-utils.core :refer :all]
            [clj-kafka.consumer.simple :as consumer.simple])
  (:import [kafka.javaapi.consumer SimpleConsumer]))

(deftest topic-meta-data-test
  (testing "It returns the same topic meta data as clj-kafka.consumer.simple/topic-meta-data for a single topic"
    (with-kafka {:topic "foo" :partitions 3} [zk-connect]
      (let [meta (topic-meta-data zk-connect "foo")
            meta-clj-kafka (with-consumer [consumer zk-connect]
                             (first (consumer.simple/topic-meta-data consumer ["foo"])))]
        (is (= meta meta-clj-kafka))))))


(deftest partition-meta-data-test
  (testing "It returns partition meta data for an existing topic"
    (with-kafka {:topic "foo" :partitions 3} [zk-connect]
      (let [meta (partition-meta-data zk-connect "foo")]
        (is (= (count meta) 3))
        (is (= (sort (map :partition-id meta))
               (range 3))))))

  (testing "It returns nil for a non-existing topic"
    (with-kafka {:topic "foo" :partitions 1} [zk-connect]
      (let [meta (partition-meta-data zk-connect "bar")]
        (is (= nil meta))))))

(deftest partition-offsets-test
  (testing "It returns the earliest and latest offsets for an existing partition"
    (with-kafka {:topic "foo" :partitions 1} [zk-connect]
      (produce zk-connect "foo" ["testo"])
      (is (= (partition-offsets zk-connect "foo" 0)
             {:earliest 0
              :latest 1}))))

  (testing "It returns {} for a non-existing partition"
    (with-kafka {:topic "foo" :partitions 1} [zk-connect]
      (is (= (partition-offsets zk-connect "foo" 2)
             {}))))

  (testing "It returns {} for a non-existing topic"
    (with-kafka {:topic "foo" :partitions 1} [zk-connect]
      (is (= (partition-offsets zk-connect "bar" 0)
             {})))))

(deftest topic-offsets-test
  (testing "It returns all partition-offsets of the given topic"
    (with-kafka {:topic "foo" :partitions 2} [zk-connect]
      (is (= (topic-offsets zk-connect "foo")
             {0 {:earliest 0
                 :latest 0}
              1 {:earliest 0
                 :latest 0}}))))

  (testing "It returns {} for non-existing topic"
    (with-kafka {:topic "foo"} [zk-connect]
      (is (= (topic-offsets zk-connect "bar")
             {})))))

(defn remove-broker-ids [leaders]
  (->> leaders
       (map (fn [[partition leader]]
              [partition (dissoc leader :broker-id)]))
       (into {})))

(deftest determine-leaders-test
  (testing "It returns a map of partitions to leader brokers for a given topic"
    (with-kafka {:topic "foo" :partitions 2} [zk-connect broker]
      ;; TODO: Removal of broker-ids from the leader maps should not
      ;; be necessary anymore once
      ;; https://github.com/pingles/clj-kafka/pull/73 is merged
      (is (= (remove-broker-ids (determine-leaders zk-connect "foo"))
             {0 broker
              1 broker}))))

  (testing "It returns {} for a non-existing topic"
    (with-kafka {:topic "foo"} [zk-connect broker]
      (is (= (determine-leaders zk-connect "bar")
             {})))))

(deftest leader-consumers-test
  (testing "It returns a map of delayed consumers to partitions it is leader for"
    (with-kafka {:topics {"foo" 2
                          "bar" 3
                          "qux" 4}}
      [zk-connect broker]
      (let [leaders (leader-consumers zk-connect ["foo" "qux"])]
        (is (= 1 (count leaders)))
        (let [[delayed-consumer partitions] (first leaders)]
          (with-open [consumer @delayed-consumer]
            (is (instance? SimpleConsumer consumer)))
          (is (= (set (map (juxt :topic :partition-id) partitions))
                 #{["foo" 0]
                   ["foo" 1]
                   ["qux" 0]
                   ["qux" 1]
                   ["qux" 2]
                   ["qux" 3]})))))))
