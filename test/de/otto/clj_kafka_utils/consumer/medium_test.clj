(ns de.otto.clj-kafka-utils.consumer.medium-test
  (:require [clojure.test :refer :all]
            [de.otto.clj-kafka-utils.test-helpers :refer :all]
            [de.otto.clj-kafka-utils.core :refer :all]
            [de.otto.clj-kafka-utils.consumer.medium :as medium]))

(defn str-val [msg]
  (String. (:value msg)))

(def test-kafka
  {:topic "foo" :partitions 1})

(defn test-consumer [zk-connect]
  (medium/consumer zk-connect "test-consumer" (:topic test-kafka) 0 1024))

(deftest medium-consumer-test
  (testing "It consumes existing messages in order"
    (with-kafka test-kafka [zk-connect]
      (produce zk-connect "foo" ["bar" "baz"])
      (with-open [consumer (test-consumer zk-connect)]
        (let [messages (medium/messages consumer 0)]
          (is (= (str-val (first messages)) "bar"))
          (is (= (str-val (second messages)) "baz"))))))

  (testing "It consumes messages as they appear"
    (with-kafka test-kafka [zk-connect]
      (produce zk-connect "foo" ["bar"])
      (with-open [consumer (test-consumer zk-connect)]
        (let [messages (medium/messages consumer 0)]
          (is (= (str-val (first messages)) "bar"))
          (produce zk-connect "foo" ["baz"])
          (is (= (str-val (second messages)) "baz"))))))

  (testing "It ends the message stream when the consumer is closed"
    (with-kafka test-kafka [zk-connect]
      (produce zk-connect "foo" ["bar"])
      (with-open [consumer (test-consumer zk-connect)]
        (let [messages (medium/messages consumer 0)]
          (is (= (str-val (first messages)) "bar"))
          (.close consumer)
          (is (= 1 (count (doall messages)))))))))


