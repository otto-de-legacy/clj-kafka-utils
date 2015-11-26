(ns ^:no-doc de.otto.clj-kafka-utils.test-helpers
  (:require [clj-kafka.core :refer [with-resource]]
            [clj-kafka.zk :as zk]
            [clj-kafka.admin :as admin]
            [clj-kafka.producer :as producer]
            [clojure.data.json :as json]
            [clojure.string :as str]
            [clojure.test :refer :all])
  (:import kafka.server.KafkaConfig
           kafka.utils.MockTime
           kafka.utils.TestZKUtils
           kafka.utils.TestUtils
           kafka.zk.EmbeddedZookeeper))

;; Embedded Zookeeper and Kafka code based on https://gist.github.com/asmaier/6465468
(defn kafka-server []
  (let [props (TestUtils/createBrokerConfig 0 (TestUtils/choosePort) true)]
    (TestUtils/createServer
      (KafkaConfig. props)
      (MockTime.))))

(defn zk-server []
  (EmbeddedZookeeper. (TestZKUtils/zookeeperConnect)))

(defn produce [zk-connect topic msgs]
  (with-open [p (producer/producer {"metadata.broker.list" (zk/broker-list (zk/brokers zk-connect))
                                    "serializer.class"     "kafka.serializer.DefaultEncoder"
                                    "partitioner.class"    "kafka.producer.DefaultPartitioner"})]
    (doseq [msg msgs]
      (producer/send-message p (producer/message topic (.getBytes msg))))))

(defn await-meta-data-propagation [server topic partition-count]
  (dotimes [partition partition-count]
    (TestUtils/waitUntilMetadataIsPropagated
      (scala.collection.JavaConversions/asScalaBuffer [server])
      topic
      partition
      5000)))

(defmacro with-kafka [options [zk-connect broker] & body]
  (let [shutdown '.shutdown
        broker   (or broker (gensym 'broker))]
    `(let [options#    ~options
           topics#     (or (:topics options#)
                           {(:topic options#)
                            (:partitions options# 1)})]
       (with-resource [zk-server# (zk-server)]
         ~shutdown
         (with-resource [kafka-server# (kafka-server)]
           ~shutdown
           (let [zk-connect-str# (.connectString zk-server#)
                 ~zk-connect     {"zookeeper.connect" zk-connect-str#}
                 ~broker         (-> (zk/brokers ~zk-connect)
                                     (first)
                                     (select-keys [:host :port]))
                 ~broker         (assoc ~broker
                                        :connect (str (:host ~broker) ":" (:port ~broker)))]
             (with-open [zk# (admin/zk-client (.connectString zk-server#))]
               (doseq [[topic# partitions#] topics#]
                 (admin/create-topic zk# topic# {:partitions partitions#}))
               (doseq [[topic# partitions#] topics#]
                 (await-meta-data-propagation kafka-server# topic# partitions#)))
             ~@body))))))
