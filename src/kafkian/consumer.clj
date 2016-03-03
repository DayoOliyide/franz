(ns ^{:doc "Clojure interface for Kafka Consumer API. For\n  complete JavaDocs\nsee:\n  http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/package-summary.html"}
    kafkian.consumer
  (:require [kafkian.data :refer :all])
  (:import java.util.List
           java.util.regex.Pattern
           [org.apache.kafka.clients.consumer ConsumerRebalanceListener KafkaConsumer OffsetAndMetadata OffsetCommitCallback]
           [org.apache.kafka.common.serialization ByteArrayDeserializer Deserializer StringDeserializer]
           org.apache.kafka.common.TopicPartition))


(defn string-deserializer [] (StringDeserializer.))
(defn byte-array-deserializer [] (ByteArrayDeserializer.))

(defn consumer
  "Takes a map of config options and return a `KafkaConsumer` for consuming records from Kafka.

  For available conifg options, see: http://kafka.apache.org/documentation.html#newconsumerconfigs

  Usage:

  (def config {\"bootstrap.servers\" \"localhost:9092\"
                       \"group.id\" \"data-pipe\"
                       \"auto.commit.interval.ms\" \"1000\"})
  (with-open [c (consumer config (string-deserializer) (string-deserializer))]
    (take 5 (messages c :topic \"test\")))
  "
  ([^java.util.Map config]
   (KafkaConsumer. config))
  ([^java.util.Map config ^Deserializer key-deserializer ^Deserializer value-deserializer]
   (KafkaConsumer. config key-deserializer value-deserializer)))


(defn subscribe-to-topics
  "Subscribes the consumer to all or some of the partitions of the topics given.
   The provided topics can be a String, a sequence of Strings or a Regular expression.
   The actual partitions subscribed to is dependant on the number of consumers using the
   same group.id and the broker managing the group.id.
   This function is equivalent to the old High level consumer

  The provided callback functions should be of a single arity and should expect a sequence of maps describing
  specific partitions (e.g [{:topic \"dev\" :partition 1} {:topic \"dev\" :partition 2}])
  These functions are called whenever the broker revokes or assigns partitions to the consumer

  (subscribe-to-topics consumer \"dev\")
  (subscribe-to-topics consumer [\"dev\" \"test\"])
  (subscribe-to-topics consumer #\"dev.+\")

  (subscribe-to-topics consumer \"dev\" :assigned-callback (fn [p] (println \"PartitionsAssigned:\"(doall p)))
                                        :revoked-callback (fn [p] (println \"PartitionsRevoked:\"(doall p))))

  NOTE:
    Topic subscription can be done in the following ways
    1) One or more topic names                        <--- Supported by this function
    2) A regular expression matching required topics  <--- Supported by this function
    3) Specific topic partitions                      <--- Supported by the subscribe-to-partitions function

    The 3 ways are mutually exclusive, meaning you can't use the same consumer to subscribe in the
    3 different ways at the same time. You either unsubscribe before re-subscribing or you use different consumers.

  For more details on usage
  http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#subscribe(java.util.List)
  http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#subscribe(java.util.List,%20org.apache.kafka.clients.consumer.ConsumerRebalanceListener)
  http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#subscribe(java.util.regex.Pattern,%20org.apache.kafka.clients.consumer.ConsumerRebalanceListener)
       "
  [^KafkaConsumer consumer topics & {:keys [assigned-callback revoked-callback]
                                     :or {assigned-callback (fn [_])
                                          revoked-callback (fn [_])}}]
  (let [listener (reify ConsumerRebalanceListener
                   (onPartitionsAssigned [_ partitions] (assigned-callback (map to-clojure partitions)))
                   (onPartitionsRevoked [_ partitions] (revoked-callback (map to-clojure partitions))))
        topics (cond
                 (sequential? topics) topics
                 (= Pattern (type topics)) topics
                 (string? topics) (vector topics)
                 :else (throw (ex-info "Topic should be a string, sequence or pattern" {:topic topics})))]
    (.subscribe consumer topics listener)))


(defn subscribe-to-partitions
  "Subscribes the consumer to the specific Topic partitions. This subscription is manual and not under the control of the broker.
   This function is equivalent to the old Simple level consumer

  (subscribe-to-partitions consumer {:topic \"dev\" :partition 2}
                                    {:topic \"dev\" :partition 0})


  NOTE:
    Topic subscription can be done in the following ways
    1) One or more topic names                        <--- Supported by the subscribe-to-topics function
    2) A regular expression matching required topics  <--- Supported by the subscribe-to-topics function
    3) Specific topic partitions                      <--- Supported by this function

    The 3 ways are mutually exclusive, meaning you can't use the same consumer to subscribe in the 3 different ways at the same time. You either unsubscribe before re-subscribing or you use different consumers.

  For more details on usage http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#assign(java.util.List)"
  [^KafkaConsumer consumer & tps]
  (let [tp-seq (map map->topic-partition tps)]
    (.assign consumer tp-seq)))


(defn subscribed-topics
  "Returns a set of topics (Strings) currently subscribed to via the subscribe-to-topics function.
   NOTE: use subscibed-partitions to get the topic and partition subscribed to via the subscribe-to-partitions function"
  [^KafkaConsumer consumer]
  (.subscription consumer))

(defn subscribed-partitions
  "Returns a set of maps representing each topic-partition that the consumer is currently subscribed to,
   either via the subscribe-to-topics or subscribe-to-partitions function.
  NOTE: If subscribed-topics is used to subscribed, there may not be any subscribed partitions as the assignment/subscription is up to the broker managing the group.id.

  "
  [^KafkaConsumer consumer]
  (reduce #(conj %1 (to-clojure %2)) #{} (.assignment consumer)))

(defn unsubscribe
  "Unsubcribes the consumer from any subscribed topics and/or partitions.
   It works for subscriptions carried out via subscribe-to-topics or subscribe-to-partitions functions"
  [^KafkaConsumer consumer]
  (.unsubscribe consumer))

(defn seek
  "Seeks the consumer offset to given offset on the topic-partitions.
   NOTE:
   The topic-partition can be given as 2 arguments, the topic (string) and partition (int)
   or it can be given as 1 argument, which is a map sequence e.g '({:topic \"topic\" :partition 2}).
   The offset can be a long, :beginning or :end.

  Usage:

  (seek consumer \"topic-a\" 23 7)
  (seek consumer \"topic-b\" 23 :beginning)
  (seek consumer \"topic-c\" 23 :end)

  (seek consumer [{:topic \"topic-a\" :partition 23}
                  {:topic \"topic-b\" :partition 23}
                  {:topic \"topic-c\" :partition 23}] 7)

  (seek consumer [{:topic \"topic-a\" :partition 23}
                  {:topic \"topic-b\" :partition 23}
                  {:topic \"topic-c\" :partition 23}] :beginning)

  (seek consumer [{:topic \"topic-a\" :partition 23}
                  {:topic \"topic-b\" :partition 23}
                  {:topic \"topic-c\" :partition 23}] :end)

  "
  ([^KafkaConsumer consumer topic partition offset]
   (seek consumer (vector {:topic topic :partition partition}) offset))
  ([^KafkaConsumer consumer tp-seq offset]
   (let [tp-class-seq (map map->topic-partition tp-seq)
         tp-class-array (into-array TopicPartition tp-class-seq)]
     (cond
       (= :beginning offset) (.seekToBeginning consumer tp-class-array)
       (= :end offset) (.seekToEnd consumer tp-class-array)
       (integer? offset) (run! #(.seek consumer % offset) tp-class-seq)
       :else (throw (ex-info "offset should be :beginning :end or a number"
                             {:offset offset}))))))

(defn messages
  "Consumes messages from subscribed partitions and returns a sequence of messages.
  If no messages are available, it will use the provided timeout (or default of 1000ms) to BLOCK for messages to be available,
  before returning.

  This function can be used to
  1) Consume from the currently subscribed topics/partitions
  2) Subscribe to a given topic(s) (or topics matching regex) and consume from the broker assigned partitions
  3) Subscribe to only the specific partition and consume from the commited offset or a specific offset

  If the consumer is already subscribed and this function is passed subscription arguments
  ( topic(s) or partitions ) which are different from it's current subscription, this function will attempt
  to change the subscription. NOTE you can't switch between topic name, regex and partition based subscriptions
  (see subscribe-to-partitions subscribe-to-topics documentation).

  Usage:

  (messages consumer)
  (messages consumer :timeout 1500)
  (messages consumer :topic \"topic-a\")
  (messages consumer :topic [\"topic-a\" \"topic-b\"])
  (messages consumer :topic #\"topic.+\")
  (messages consumer :topic \"topic-a\" :partition 2)
  (messages consumer :topic \"topic-a\" :partition 2 :timeout 3000)
  (messages consumer :topic \"topic-a\" :partition 2 :offset 63)
  (messages consumer :topic \"topic-a\" :partition 2 :offset :beginning)
  (messages consumer :topic \"topic-a\" :partition 2 :offset :end)
  (messages consumer :topic \"topic-a\" :partition 2 :offset :end :timeout 1500)

  "
  ([^KafkaConsumer consumer] (messages consumer :timeout 1000))
  ([^KafkaConsumer consumer & {:keys [topic partition offset timeout]
                               :or {timeout 1000}
                               :as options}]

   (when (and (some? partition) (nil? topic))
     (throw (ex-info "Topic needed to subscribe to partition" options)))

   (when (and (some? offset) (or (nil? topic) (nil? partition)))
     (throw (ex-info "Topic and partition need to seek to offset" options)))

   (when (and (= Pattern (type topic)) (some? partition))
     (throw (ex-info "Using a regex for topic with a partition is not possible" options)))

   (when topic
     (if partition
       (do  (subscribe-to-partitions consumer {:topic topic :partition partition})
            (when offset
              (seek consumer topic partition offset)))
       (subscribe-to-topics consumer topic)))

   (let [consumer-records (.poll consumer timeout)]
     (to-clojure consumer-records))))



(defn commit-async
  "Commits the offsets of messages returned by the last call to the messages function or the given offsets.
  This is done aysnchronously and will return immediately.
  (Based on the code in kafka-clients 0.9.0.0 the commit request is not
   actually made until next time the messages function is called)

  Usage:

  1) Commits all the offsets received from the last call to the messages function.
     Exceptions/Errors are ignored

  (commit-async consumer)



  2) Commits all the offsets received from the last call to the messages function.
     Success or failure is handled by the given callback function

  (commit-async consumer (fn [offsets exception]
                          (if exception
                             (println \"Commits failed for \" offsets \" Exception->\" exception)
                             (println \"Commits passed for \" offsets))))



  3) Commits the specified offsets to the specific topic-partitions.
     Success or failure is handled by the given callback function

  (def tp-om   {{:topic \"topic-a\", :partition 4} {:offset 24, :metadata \"important commit\"},
                {:topic \"topic-a\", :partition 1} {:offset 234, :metadata \"commited by thread A\"},
                {:topic \"topic-b\", :partition 7} {:offset 23, :metadata \"commited on 12/12/12\"}})

  (commit-async consumer tp-om (fn [offsets exception]
                                (if exception
                                   (println \"Commits failed for \" offsets \" Exception->\" exception)
                                   (println \"Commits passed for \" offsets))))
  "
  ([^KafkaConsumer consumer] (.commitAsync consumer))
  ([^KafkaConsumer consumer offset-commit-fn]
   (let [callback (reify OffsetCommitCallback
                    (onComplete [_ offsets exception]
                      (offset-commit-fn (tp-om-map->map offsets) exception)))]
     (.commitAsync consumer callback)))
  ([^KafkaConsumer consumer topic-partition-offsets-metadata offset-commit-fn]
   (let [callback (reify OffsetCommitCallback
                    (onComplete [_ offsets exception]
                      (offset-commit-fn (tp-om-map->map offsets) exception)))
         tp-om-map (map->tp-om-map topic-partition-offsets-metadata)]
     (.commitAsync consumer tp-om-map callback))))


(defn commit-sync
  "Commits the offsets of messages returned by the last call to the messages function or the given offsets.
  This is done synchronously and will block until success or failure (Exception thrown)

  Usage:

  1) Commits all the offsets received from the last call to the messages function.
     If there's any failure, an Exception is thrown.

  (commit-sync consumer)


  2) Commits the specified offsets to the specific topic-partitions.
     If there's any failure, an Exception is thrown.


  (def tp-om   {{:topic \"topic-a\", :partition 4} {:offset 24, :metadata \"important commit\"},
                {:topic \"topic-a\", :partition 1} {:offset 234, :metadata \"commited by thread A\"},
                {:topic \"topic-b\", :partition 7} {:offset 23, :metadata \"commited on 12/12/12\"}})

  (commit-sync consumer tp-om)
  "
  ([^KafkaConsumer consumer] (.commitSync consumer))
  ([^KafkaConsumer consumer topic-partitions-offsets-metadata]
   (let [tp-om-map (map->tp-om-map topic-partitions-offsets-metadata)]
     (.commitSync consumer tp-om-map))))


(defn last-committed-offset
  "Gets the last committed offset for the partition of a topic.
   This function may block,
   see http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#committed(org.apache.kafka.common.TopicPartition)

  Usage:

  (last-committed-offset consumer {:topic \"topic-a\" :partition 2})
  "
  [^KafkaConsumer consumer tp]
  (->> tp
       map->topic-partition
       (.committed consumer)
       to-clojure))


(defn list-all-topics
  "Get metadata about all partitions for all topics that the user is authorized to view.
   This function may block for a short time.
   See http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#listTopics()

  "
  [^KafkaConsumer consumer]
  (str-pi-map->map (.listTopics consumer)))

(defn list-all-partitions
  "Get metadata about all partitions for a particular topic.
   This function may block for a short time.
   See http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#partitionsFor(java.lang.String)

"
  [^KafkaConsumer consumer topic]
  (map to-clojure  (.partitionsFor consumer topic)))


(defn pause
  "Stops messages being consumed from the given partitions.
   This takes effect on the next call on the messages function
   See http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#pause(org.apache.kafka.common.TopicPartition...)

  Usage:

  (pause consumer {:topic \"topic-a\" :partition 2}
                  {:topic \"topic-b\" :partition 0})
  "
  [^KafkaConsumer consumer tp-seq]
  (->> (map map->topic-partition tp-seq)
       (into-array TopicPartition)
       (.pause consumer)))


(defn resume
  "Resumes messages being consumed from the given partitions.
   This takes effect on the next call on the messages function
   See http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html#resume(org.apache.kafka.common.TopicPartition...)

  Usage:

  (resume consumer {:topic \"topic-a\" :partition 2}
                   {:topic \"topic-b\" :partition 0})
  "
  [^KafkaConsumer consumer tp-seq]
  (->> (map map->topic-partition tp-seq)
       (into-array TopicPartition)
       (.resume consumer)))


(defn metrics
  "TODO"
  [^KafkaConsumer consumer]
  )
