(ns ^{:doc "Clojure interface for Kafka Producer API. For
  complete JavaDocs, see:
  http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/package-summary.html"}
    kafkian.producer
  (:refer-clojure :exclude [send flush])
  (:require [kafkian.data :refer :all])
  (:import [java.util.concurrent Future TimeUnit TimeoutException]
           [org.apache.kafka.clients.producer Callback KafkaProducer ProducerRecord RecordMetadata]
           [org.apache.kafka.common Metric MetricName]
           [org.apache.kafka.common.serialization Serializer ByteArraySerializer StringSerializer]))



(defn- map-future-val
  [^Future fut f]
  (reify
    java.util.concurrent.Future
    (cancel [_ interrupt?] (.cancel fut interrupt?))
    (get [_] (f (.get fut)))
    (get [_ timeout unit] (f (.get fut timeout unit)))
    (isCancelled [_] (.isCancelled fut))
    (isDone [_] (.isDone fut))))



(defn string-serializer [] (StringSerializer.))
(defn byte-array-serializer [] (ByteArraySerializer.))

(defn producer
  "Takes a map of config options and returns a `KafkaProducer` for publishing records to Kafka.

  NOTE `KafkaProducer` instances are thread-safe and should generally be shared for best performance.

  For more information and available config options,
  see: http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/producer/KafkaProducer.html
       http://kafka.apache.org/documentation.html#producerconfigs


  Usage:

;; Created using just a map of configs, in this case the keys
;; bootstrap.servers value.serializer and key.serializer are required
 (producer {\"bootstrap.servers\" \"localhost:9092\"
            \"value.serializer\" \"org.apache.kafka.common.serialization.StringSerializer\"
            \"key.serializer\" \"org.apache.kafka.common.serialization.StringSerializer\"})

;; Created using a map of configs and the serializers for keys and values.
 (producer {\"bootstrap.servers\" \"localhost:9092\"} (string-serializer) (string-serializer))

;; KafkaProducer should be closed when not used anymore, as it's closeable,
;; it can be used in the with-open macro
  (def config {\"bootstrap.servers\" \"localhost:9092\"})
  (with-open [p (producer config (string-serializer) (string-serializer))]
    (-> (send p (record \"topic-a\" \"Hello World\"))
        (.get)))
  "

  ([^java.util.Map config]
   (KafkaProducer. config))
  ([^java.util.Map config ^Serializer key-serializer ^Serializer value-serializer]
   (KafkaProducer. config key-serializer value-serializer)))

(defn record
  "Return a record that can be published to Kafka using [[send]]."
  ([topic value]
   (ProducerRecord. topic value))
  ([topic key value]
   (ProducerRecord. topic key value))
  ([topic partition key value]
   (ProducerRecord. topic partition key value)))

(defn send
  "Asynchronously send a record to Kafka. Returns a `Future` of a map
  with `:topic`, `:partition` and `:offset` keys. Optionally provide
  a callback fn that will be called when the operation completes.
  Callback should be a fn of two arguments, a map as above, and an
  exception. Exception will be nil if operation succeeded.

  See: http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#send(org.apache.kafka.clients.producer.ProducerRecord)

  Usage:

  ;;To send the message asynchronously and return a Future
  (send producer (record \"topic-a\" \"Test message 1\"))
  ;; => #object[string representation of future object]

  ;;To send message synchronously, deref the returned Future
  @(send producer (record \"topic-a\" \"Test message 2\"))
  ;; => {:topic \"topic-a\", :partition 4, :offset 0}

  ;;To send the message asynchronously and provide a callback
  ;;returns the future.
  (send producer (record \"topic-a\" \"Test message 3\") #(println \"Metadata->\" %1 \"Exception->\" %2))
  ;; => #object[string representation of future object]
  ;; Metadata-> {:topic topic-unknown, :partition 4, :offset 1} Exception-> nil
  "
  ([^KafkaProducer producer record]
   (let [fut (.send producer record)]
     (map-future-val fut to-clojure)))
  ([^KafkaProducer producer record callback]
   (let [fut (.send producer record (reify Callback
                                      (onCompletion [_ metadata exception]
                                        (callback (and metadata (to-clojure metadata)) exception))))]
     (map-future-val fut to-clojure))))

(defn flush
  "See: http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#flush()"
  [^KafkaProducer producer]
  (.flush producer))

(defn close
  "Like `.close`, but with a default time unit of ms for the arity with timeout.

  See:

  - http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close()
  - http://kafka.apache.org/090/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html#close(long,%20java.util.concurrent.TimeUnit)"
  ([^KafkaProducer producer]
   (.close producer))
  ([^KafkaProducer producer timeout-ms]
   (.close producer timeout-ms TimeUnit/MILLISECONDS)))

(defn partitions
  "Returns a sequence of maps which represent information about each partition of the
  specified topic.

  Usage :

  (partitions producer \"topic-a\")
  ;; => [{:topic \"topic-a\",
  ;;      :partition 2,
  ;;      :leader {:id 2, :host \"172.17.0.3\", :port 9093},
  ;;      :replicas [{:id 2, :host \"172.17.0.3\", :port 9093}
  ;;                 {:id 3, :host \"172.17.0.5\", :port 9094}],
  ;;      :in-sync-replicas [{:id 2, :host \"172.17.0.3\", :port 9093}
  ;;                         {:id 3, :host \"172.17.0.5\", :port 9094}]}
  ;;     {:topic \"topic-a\",
  ;;      :partition 1,
  ;;      :leader {:id 1, :host \"172.17.0.4\", :port 9092},
  ;;      :replicas [{:id 1, :host \"172.17.0.4\", :port 9092}
  ;;                 {:id 2, :host \"172.17.0.3\", :port 9093}],
  ;;      :in-sync-replicas [{:id 1, :host \"172.17.0.4\", :port 9092}
  ;;                         {:id 2, :host \"172.17.0.3\", :port 9093}]}]
  "
  [^KafkaProducer producer topic]
  (mapv to-clojure (.partitionsFor producer topic)))

(defn metrics
  "Returns a map data structure representing all the producer's internal metrics.

   The map structure is unfortunately quite dense and is essentially made of maps
   containing keys mapping to maps recursively.

   The structure can be viewed as
   Level 1:  {:group metric-group-name} -mapping-to-> {{:name metric-name} {}}
   Level 2:  {:name  metric-name}       -mapping-to-> {{:tags map-of-tags} {}}
   Level 3:  {:tags  map-of-tags}       -mapping-to-> {:description metric-description
                                                       :value metric-value}

  Usage :

  ;The following is a simplified result showing only the data
  ;for the metric response-rate under the producer-metrics group and tagged with
  ;with a client-id of producer-1.

  (metrics producer)
  ;; => {{:group \"producer-metrics\"} {{:name \"response-rate\"} {{:tags {\"client-id\" \"producer-1\"}} {:description \"Responses received sent per second.\" :value 0.0}}}}


  ;To help in navigating such a dense structure, there's the metrics-lens function in the
  ; kafkian.utility-belt namespace

  (use 'kafkian.utility-belt)
  (def metrics-map (metrics producer))

  (metrics-lens metrics-map :group \"producer-metrics\" :name \"response-rate\" :tags {\"client-id\" \"producer-1\"})
  ;; => {:description \"Responses received sent per second.\", :value 0.0}

  (metrics-lens metrics-map :group :ONLY)
  ;; => (\"producer-node-metrics\" \"producer-metrics\" \"producer-topic-metrics\")
  "
  [^KafkaProducer producer]
  (metrics->map (.metrics producer))
  )
