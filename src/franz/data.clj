(ns franz.data
  (:import [java.util HashMap Map]
           [org.apache.kafka.clients.consumer ConsumerRecord ConsumerRecords OffsetAndMetadata]
           [org.apache.kafka.common Node PartitionInfo TopicPartition]))

(defprotocol ToClojure
  ""
  (to-clojure [x] "Converts type to Clojure structure"))


(extend-protocol ToClojure

  nil
  (to-clojure [x] nil)

  TopicPartition
  (to-clojure [x]
    {:topic (.topic x)
     :partition (.partition x)})

  ConsumerRecord
  (to-clojure [x]
    {:topic (.topic x)
     :partition (.partition x)
     :offset (.offset x)
     :key (.key x)
     :value (.value x)})

  ConsumerRecords
  (to-clojure [x]
    (map to-clojure x))

  OffsetAndMetadata
  (to-clojure [x]
    {:offset (.offset x)
     :metadata (.metadata x)})

  Node
  (to-clojure [x]
    {:id (.id x)
     :host (.host x)
     :port (.port x)})

  PartitionInfo
  (to-clojure [x]
    {:topic (.topic x)
     :partition (.partition x)
     :leader (to-clojure (.leader x))
     :replicas (mapv to-clojure (.replicas x))
     :in-sync-replicas  (mapv to-clojure (.inSyncReplicas x))}))


(defn clojure->topic-partition [{:keys [topic partition] :as m}]
  (if (or (nil? topic) (nil? partition))
    (throw (ex-info "Provided map is missing topic or partition keys" m))
    (TopicPartition. topic partition)))

(defn clojure->offset-metadata [{:keys [offset metadata] :as m}]
  (if (or (nil? offset) (nil? metadata))
    (throw (ex-info "Provided map is missing offset or metadata keys" m))
    (OffsetAndMetadata. offset metadata)))

(defn tp-om-map->clojure
  "Takes a java.util.Map made of TopicPartition as keys and OffsetAndMetadata as values,
   converts them to the following clojure equivalent data structure 

  {{:topic \"test\", :partition 77} {:offset 34, :metadata \"data data\"},
   {:topic \"prod\", :partition 4} {:offset 24, :metadata \"more data\"},
   {:topic \"dev\", :partition 1} {:offset 234, :metadata \"loads of data\"},
   {:topic \"dev\", :partition 7} {:offset 23, :metadata \"mega data\"}}
"
  [^Map tp-om]
  (let [reduce-fn (fn [m [^TopicPartition tp ^OffsetAndMetadata om]]
                    (assoc m (to-clojure tp) (to-clojure om)))]
    (reduce reduce-fn {} tp-om)))

(defn clojure->tp-om-map
  "Takes a Clojure map (see below for example) and converts it to a java.util.Map made of TopicPartition as keys and OffsetAndMetadata as values

  {{:topic \"test\", :partition 77} {:offset 34, :metadata \"data data\"},
   {:topic \"prod\", :partition 4} {:offset 24, :metadata \"more data\"},
   {:topic \"dev\", :partition 1} {:offset 234, :metadata \"loads of data\"},
   {:topic \"dev\", :partition 7} {:offset 23, :metadata \"mega data\"}}
  "
  [m]
  (let [tp-om-map (HashMap.)
        reduce-fn (fn [^Map m kv]
                    (.put m (clojure->topic-partition (first kv))
                          (clojure->offset-metadata (second kv)))
                    m)]
    (reduce reduce-fn tp-om-map  m)))


(defn str-pi-map->clojure
  "Takes a java.util.Map made of Strings as keys and java.util.List <PartitionInfo> as values,
   converts it to the following clojure equivalent data structure

  {
\"topic-a\"
 [{:topic \"topic-a\",
   :partition 1,
   :leader {:id 1, :host \"172.17.0.3\", :port 9092},
   :replicas [{:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093} {:id 3, :host \"172.17.0.4\", :port 9094}],
   :in-sync-replicas [{:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093} {:id 3, :host \"172.17.0.4\", :port 9094}]}
  {:topic \"topic-a\",
   :partition 0,
   :leader {:id 3, :host \"172.17.0.4\", :port 9094},
   :replicas [{:id 3, :host \"172.17.0.4\", :port 9094} {:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093}],
   :in-sync-replicas [{:id 3, :host \"172.17.0.4\", :port 9094} {:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093}]}],

 \"topic-b\"
 [{:topic \"topic-b\",
   :partition 0,
   :leader {:id 3, :host \"172.17.0.4\", :port 9094},
   :replicas [{:id 3, :host \"172.17.0.4\", :port 9094} {:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", 
:port 9093}],
   :in-sync-replicas [{:id 3, :host \"172.17.0.4\", :port 9094} {:id 1, :host \"172.17.0.3\", :port 9092} {:id 2, :host \"172.17.0.2\", :port 9093}]}]}
  "
  [^Map str-pi]
  (let [reduce-fn (fn [m [name pi-list]]
                    (assoc m name (mapv to-clojure pi-list)))]
    (reduce reduce-fn {} str-pi)))



