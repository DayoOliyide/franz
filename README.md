# franz

A Clojure wrapper library for the Kafka 0.9 clients.  

**NOTE**:This library is NOT compatible with Kafka brokers below version 0.9

## Installation

TODO

## Usage

### Producer

```
clojure
(require '[franz.producer :as kp])

(with-open [p (kp/producer {"bootstrap.servers" "localhost:9092"} (kp/string-serializer) (kp/string-serializer))]
  @(kp/send p (kp/record "topic-a" "Hola Mundo!")))
```

### Consumer

```
clojure
(require '[franz.consumer :as kc])

(with-open [c (kc/consumer {"bootstrap.servers" "localhost:9092"
                            "group.id" "consumer-id"} (kc/string-deserializer) (kc/string-deserializer))]
  (kc/messages c :topic "topic-a"))
```


## License

Copyright © 2016 

Distributed under the Apache License v 2.0 (http://www.apache.org/licenses/LICENSE-2.0)  
