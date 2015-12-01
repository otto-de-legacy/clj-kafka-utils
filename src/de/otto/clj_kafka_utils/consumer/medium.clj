(ns de.otto.clj-kafka-utils.consumer.medium
  "A medium-level consumer which--as the name suggests--is slightly
  higher level than the simple consumer but still lower level than the
  high-level consumer.

  Compared to the simple consumer, the differences are:

  1. It handles connection errors and leader changes
  2. The [[messages]] lazy-seq is open-ended, i.e. it spans across more than just a single chunk
  3. It is bound to a topic and partition at construction time"
  (:require [clj-kafka.core :as kafka]
            [clj-kafka.consumer.simple :as consumer.simple]
            [de.otto.clj-kafka-utils.core :refer [determine-leaders]]
            [clojure.tools.logging :as log])
  (:import [java.io Closeable]
           [kafka.javaapi.consumer SimpleConsumer]
           [kafka.javaapi FetchResponse OffsetRequest OffsetResponse]
           [kafka.message MessageAndOffset]
           [kafka.api FetchRequest FetchRequestBuilder PartitionOffsetRequestInfo]
           [kafka.common ErrorMapping TopicAndPartition]))

;; This protocol only exists to make the consumer implementation below
;; more convenient.
(defprotocol ^:private IConsumer
  (^:private get-consumer [this reinitialize?])
  (^:private with-response [this request request-fn response-fn closed-fn])
  (^:private messages-chunk [this offset])
  (^:private messages-chunk-retrying [this offset])
  (topic-offset [this offset-position]
    "Returns the offset at `offset-position` (either `:earliest` or
    `:latest`) for the consumer topic's partition.")
  (messages [this start-offset]
    "Returns a lazy-seq of messages from the consumer topic's
    partition starting at `start-offset` in the same format as
    `clj-kafka.consumer.simple/messages`. It is open-ended, i.e. it
    will fetch more than just one message chunk and will keep retrying
    to fetch more when reaching the end of the partition with a delay
    according to `fetch-retry-delay`."))

(defn ^:private message->map
  "Since clj-kafka's to-kafka doesn't pass through nextOffset we need
  to do it ourselves here.

  With https://github.com/pingles/clj-kafka/pull/71 this will be part
  of clj-kafka itself."
  [^MessageAndOffset mo]
  (assoc (kafka/to-clojure mo)
         :next-offset (.nextOffset mo)))

(defrecord MediumConsumer [state config client-id topic partition fetch-size fetch-retry-delay reconnect-delay]
  Closeable
  (close [this]
    (swap! state
           (fn [cstate]
             (let [consumer ^SimpleConsumer (:simple-consumer cstate)]
               (if (or (not consumer) (:closed? cstate))
                 cstate
                 (do
                   (.close consumer)
                   {:closed? true
                    :simple-consumer nil}))))))

  IConsumer
  (get-consumer [this force-reinitialization?]
    (let [new-state (swap! state
                           (fn [cstate]
                             (let [consumer ^SimpleConsumer (:simple-consumer cstate)]
                               (cond (:closed? cstate)
                                     cstate

                                     (or force-reinitialization? (not consumer))
                                     (do (when consumer
                                           (try
                                             (.close consumer)
                                             (catch Exception e
                                               (log/error e "Error closing consumer")))
                                           (Thread/sleep reconnect-delay))
                                         (if-let [leader (get (determine-leaders config topic) partition)]
                                           (assoc cstate :simple-consumer
                                                  (consumer.simple/consumer (:host leader) (:port leader) client-id))
                                           (throw (ex-info "Unable to determine leader"
                                                           {:topic topic
                                                            :partition partition}))))

                                     :else
                                     cstate))))]
      (:simple-consumer new-state)))
  (with-response [this request request-fn response-fn closed-fn]
    (loop [reinitialize-consumer? false]
      (if-let [consumer (get-consumer this reinitialize-consumer?)]
        (let [response (try
                         (request-fn consumer request)
                         (catch Exception e
                           (log/error e "Error during request")
                           false))]
          (cond (not response)
                (recur true)

                (.hasError response)
                (let [error-code (.errorCode response topic partition)]
                  (condp = error-code
                    (ErrorMapping/OffsetOutOfRangeCode)
                    (throw (ex-info "Offset out of range"
                                    {:offset (.. request requestInfo head _2 offset)
                                     :partition partition
                                     :topic topic}))
                    (do (log/warn (str "Error response code " error-code
                                       " (" (.getName (class (ErrorMapping/exceptionFor error-code))) "),"
                                       " reinitializing consumer"))
                        (recur true))))

                :else
                (response-fn response)))
        (closed-fn))))
  (topic-offset [this offset-position]
    (with-response this
      (let [op (get {:latest -1 :earliest -2} offset-position)
            tp (TopicAndPartition. topic partition)
            hm (java.util.HashMap. {tp (PartitionOffsetRequestInfo. op 1)})]
        (OffsetRequest. hm (kafka.api.OffsetRequest/CurrentVersion) client-id))
      (fn [^SimpleConsumer consumer request]
        (.getOffsetsBefore consumer request))
      (fn [^OffsetResponse response]
        (first (.offsets response topic partition)))
      (fn []
        (throw (ex-info "Consumer is already closed")))))
  (messages-chunk [this offset]
    (with-response this
      (consumer.simple/fetch-request client-id topic partition offset fetch-size)
      (fn [^SimpleConsumer consumer ^FetchRequest request]
        (.fetch consumer request))
      (fn [^FetchResponse response]
        (->> (.messageSet response topic partition)
             (.iterator)
             (iterator-seq) ;; unchunk?
             (map message->map)
             (drop-while #(> offset (:offset %)))
             (seq)))
      (constantly :end)))
  (messages-chunk-retrying [this offset]
    (loop []
      (let [messages (messages-chunk this offset)]
        (when-not (= :end messages)
          (or (seq messages)
              (do (Thread/sleep fetch-retry-delay)
                  (recur)))))))
  (messages [this start-offset]
    (letfn [(step [offset tail]
              (lazy-seq
               (loop [tail tail]
                 (let [messages (or (seq tail)
                                    (messages-chunk-retrying this offset))
                       message (first messages)]
                   (when (and message (not (:closed? @state)))
                     (cons message
                           (step (:next-offset message)
                                 (rest messages))))))))]
      (step start-offset nil))))

(doseq [var [#'->MediumConsumer #'map->MediumConsumer]]
  (alter-meta! var assoc :no-doc true))

(defn consumer
  "Returns a medium consumer.

  Arguments:

  config
  : Zookeeper connection map, e.g. {\"zookeeper.connect\" \"localhost:2821\"}
  
  client-id
  : Client identifier

  topic
  : Topic the consumer will be bound to

  partition
  : Partition of the topic the consumer will be bound to

  fetch-size
  : Size (in bytes) of message chunks to fetch when consuming via `messages`

  fetch-retry-delay
  : Delay (in ms) between fetch retries when end of partition was reached

  reconnect-delay
  : Delay (in ms) between reconnect attempts in case of a connection error / leader change"
  ([config client-id topic partition fetch-size]
   (consumer config client-id topic partition fetch-size 1000 1000))
  ([config client-id topic partition fetch-size fetch-retry-delay reconnect-delay]
   (->MediumConsumer (atom {:consumer nil
                      :closed? false})
               config
               client-id
               topic
               partition
               fetch-size
               fetch-retry-delay
               reconnect-delay)))
