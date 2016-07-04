# kafkian

A Clojure library for the Kafka 0.9.X.X and 0.10.0.0 release.

This library takes it's inspiration and some code from the [clj- kafka](https://github.com/pingles/clj-kafka/) library.

Current build status: [![Build Status](https://circleci.com/gh/DayoOliyide/kafkian/tree/master.svg?style=svg)](https://circleci.com/gh/DayoOliyide/kafkian/tree/master)

**NOTE**:
This library is *NOT* compatible with Kafka Clusters below version 0.9
         Use the [clj- kafka](https://github.com/pingles/clj-kafka/) for Kafka Clusters version 0.8
         So far it's only been tested against 0.9 and 0.10 Kafka Clusters

**[CHANGELOG]**

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
  (kc/messages c))
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
