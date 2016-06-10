# kafkian

A Clojure library for the Kafka 0.9 release.

This library takes it's inspiration and some code from the [clj- kafka](https://github.com/pingles/clj-kafka/) library.

It's raison d'être is that I had an immediate need for a clojure wrapper/interface to Kafka 0.9,
which clj-kafka doesn't currently have.


**NOTE**:This library is NOT compatible with Kafka Clusters below version 0.9
         Use the [clj- kafka](https://github.com/pingles/clj-kafka/) for Kafka Clusters version 0.8

**Still a SNAPSHOT release, a propery release will be done soon**

## Installation

Add the following to your [Leiningen](http://github.com/technomancy/leiningen) `project.clj`:

![latest kafkian version](https://clojars.org/com.dayooliyide/kafkian/latest-version.svg)


## Usage

### Producer

```Clojure
(require '[kafkian.producer :as kp])

(with-open [p (kp/producer {"bootstrap.servers" "localhost:9092"}
                           (kp/string-serializer)
                           (kp/string-serializer))]
  @(kp/send p (kp/record "topic-a" "Hola Mundo!")))
```

### Consumer

```Clojure
(require '[kafkian.consumer :as kc])

(with-open [c (kc/consumer {"bootstrap.servers" "localhost:9092"
                            "group.id" "consumer-id"}
                            (kc/string-deserializer)
                            (kc/string-deserializer))]
  (kc/subscribe c "topic-a")
  (kc/messages c :topic "topic-a"))
```

### Admin

```Clojure
(require '[kafkian.admin :as ka])

(def zk-util (ka/zk-utils "localhost:2181"))

(ka/create-topic zk-util "events")

(ka/create-topic zk-util "events2" :partition 3 :replication-factor 3)

(ka/create-topic zk-util "events-store" :topic-conifg {"cleanup.policy" "compact"})

(ka/topic-exists? zk-util "events")

(ka/delete-topic zk-util "events2")
```

## License

Copyright © 2016

Distributed under the Apache License v 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
