(ns de.otto.kafka-utils.core
  "Provides convenience functions for retrieving Kafka meta data.

  Functions with a `source` argument either accept a
  `clj-kafka.consumer.simple/SimpleConsumer` instance or a Zookeeper
  connection map. In the latter case, a one-off simple consumer will
  be instantiated."
  (:require zookeeper
            [clojure.data.json :as json]
            [clj-kafka.core :refer [with-resource]]
            [clj-kafka.zk :as zk]
            [clj-kafka.consumer.simple :as consumer.simple])
  (:import [kafka.javaapi.consumer SimpleConsumer]))

(defn ^SimpleConsumer simple-consumer
  "Instantiates a SimpleConsumer from a given `broker` map with a `:host` and `:port` key."
  [broker client-id]
  (consumer.simple/consumer (:host broker) (:port broker) client-id))

(defn brokers-by-id
  "Get brokers from zookeeper

  This is the same function as clj-kafka.zk/brokers but instead of a
  seq of broker meta data it returns a map from broker ID to broker
  meta data.

  TODO: Once https://github.com/pingles/clj-kafka/pull/73 is merged
  this function can be implemented in terms of clj-kafka.zk/brokers."
  [source]
  (with-resource [z (zookeeper/connect (get source "zookeeper.connect"))]
    zookeeper/close
    (->> (zookeeper/children z "/brokers/ids")
         (map (fn [^String id]
                [(Long/valueOf id)
                 (-> (zookeeper/data z (str "/brokers/ids/" id))
                     ^bytes (:data)
                     (String.)
                     (json/read-str :key-fn keyword))]))
         (into {}))))

(defmacro with-consumer
  "Binds `consumer` to a one-off SimpleConsumer instance connected to
  the controller broker found via the given Zookeeper `config`. "
  [[consumer config] & body]
  `(let [config# ~config]
     (when-let [broker# (get (brokers-by-id config#) (zk/controller config#))]
       (with-open [~consumer (simple-consumer broker# "de.otto.kafka-utils")]
         ~@body))))

(defn topic-meta-data
  "Wraps `clj-kafka.consumer.simple/topic-meta-data` but only accepts
  a single `topic`."
  [source topic]
  (if (instance? SimpleConsumer source)
    (first (consumer.simple/topic-meta-data source [topic]))
    (with-consumer [consumer source]
      (topic-meta-data consumer topic))))

(defn partition-meta-data
  "Fetches partition meta data for the given `topic`."
  [source topic]
  (-> (topic-meta-data source topic)
      :partition-metadata
      seq))

(defn partition-offsets [source topic partition]
  "Returns a map of `:latest` and `:earliest` offset for `partition`
  of `topic`."
  (if (instance? SimpleConsumer source)
    (->> [:earliest :latest]
         (map (fn [offset-position]
                (when-let [offset (consumer.simple/topic-offset source topic partition offset-position)]
                  [offset-position offset])))
         (into {}))
    (with-consumer [consumer source]
      (partition-offsets consumer topic partition))))

(defn topic-offsets
  "Returns a map of all partitions of the given `topic` to their
  respective earliest and latest offsets (as returned by
  `partition-offsets`."
  [source topic]
  (->> (partition-meta-data source topic)
       (group-by :leader)
       (mapcat (fn [[broker partitions]]
                 (with-open [consumer (simple-consumer broker "topic-offsets")]
                   (for [{:keys [partition-id]} partitions]
                     [partition-id (partition-offsets consumer topic partition-id)]))))
       (into {})))

(defn determine-leaders [source topic]
  "Returns a map of all partitions of the given `topic` to their
  respective leaders (as returned by `partition-meta-data`)."
  (->> (partition-meta-data source topic)
       (group-by :partition-id)
       (map (fn [[partition metadata]]
              [partition (-> metadata first :leader)]))
       (into {})))

(defn leader-consumers
  "Returns a map of delayed SimpleConsumers for all leader brokers of
  the given `topics` to a seq of partitions they are leader for (as
  returned by `partition-meta-data`, enriched with the respective
  `:topic`)."
  [source topics]
  (->> topics
       (mapcat (fn [topic]
                 (map #(assoc % :topic topic)
                      (partition-meta-data source topic))))
       (group-by :leader)
       (map (fn [[leader meta]]
              [(delay (simple-consumer leader "leader-consumers")) meta]))
       (into {})))
