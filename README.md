# Reactive Kafka
A high level kafka consumer which wrapps the low level api of [Kafka Reactor](https://projectreactor.io/docs/kafka/release/reference/)
and provides a similar usability like [Spring Kafka](https://spring.io/projects/spring-kafka).

## Dependency
```gradle
implementation("com.quandoo.lib:reactive-kafka:1.2.4")
```

## Usage

### Spring
The configuration is auto-discoverable hence only the artifact has to be included in you project 
and a yaml configuration has to be added.

#### Properties
```yaml
kafka:
  bootstrap-servers: "localhost:9092"                                                     # Kafka servers
  security-protocol: "SSL"                                                                # Security protocol used (Default: PLAINTEXT)
  client-dns-lookup: "use_all_dns_ips"                                                    # Dns lookup (Default: use_all_dns_ips)
  consumer:
    group-id: ${spring.application.name}                                                  # Kafka groupId
    parallelism: 1                                                                        # How many parallel consumptions (Default: 1)
    auto-offset-reset: earliest                                                           # Offset reset (Default: latest)
    batch-size: 10                                                                        # Max number of messages per one batch (Default: 10)
    partition-assignment-strategy: "org.apache.kafka.clients.consumer.RangeAssignor"      # How to assign partitions (Default: org.apache.kafka.clients.consumer.RangeAssignor)
    batch-wait-millis: 200                                                                # Max waiting time until processing happens if the size wasn't matched (Default: 200)
    retry-backoff-millis: 100                                                             # How long to backoff until retrying again (Default: 100)
    max-pool-interval-millis: 300000                                                      # Max interval between 2 pools (Default: 300000)
  producer:
    max-in-flight: 10                                                                     # Max number of message un-ackd
  
  # Documented in official kafka client
  ssl:
    endpoint-identification-algorithm: ""
    protocol: ""
    enabled-protocols: ""
    provider: ""
    cypher-suites: ""
    keystore-type: ""
    keystore-location: ""
    keystore-password: ""
    key-password: ""
    truststore-type: ""
    truststore-location: ""
    truststore-password: ""
    keymanager-algorithm: ""
    trustmanager-algorithm: ""
    secure-random-implementation: ""
  # Documented in official kafka client
  sasl:
    mechanism: ""
    jaas: ""
    client-callback-handler-class: ""
    login-callback-handler-class: ""
    login-class: ""
    kerbos-service-name: ""
    kerbos-kinit-cmd: ""
    kerbos-ticket-renew-window-factor: 0.5
    kerbos-ticket-renew-jitter: 0.5
    kerbos-min-time-before-relogin: 100
    login-refresh-window-factor: 100
    login-refresh-window-jitter: 100
    login-refresh-min-period-seconds: 10
    login-refresh-buffer-seconds: 10
```

All consumer properties can be also specified/overloaded in the listener annotation.

#### Consumer configuration
The function which is handling the message has to return RxJava2 Completable or Reactor Mono<Void>.
The name parameter is putting the listeners and filters in a group. Filters will apply to listeners which have the same name.

##### Single Listener 
```java
      // Topics support SPEL
      @KafkaListener(groupId = "test-consumer", topics = {"topic1", "topic2"}, valueType = DTO.class)
      public Completable processMessage(final ConsumerRecord<String, DTO> message) {
          // Do something
      }
```

##### Batch Listener 
```java
      // Topics support SPEL
      @KafkaListener(groupId = "test-consumer", topics = {"topic1", "topic2"}, valueType = DTO.class)
      public Mono<Void> processMessage(final List<ConsumerRecord<String, DTO>> messages) {
          // Do something
      }
```

##### Filter
Allows to filter the message after key and value deserializer
```java
      @Component
      @KafkaListenerFilter(groupId = "test-consumer", valueClass = DTO.class)
      public class VersionFilter implements Predicate<ConsumerRecord<Object, Object>> {
      
          @Override
          Boolean apply(ConsumerRecord<Object, Object> receiverRecord) {
              return true
          }
      }
```

##### Pre-Filter
Allows to filter the message before the key and value deserializers kick in
```java
      @Component
      @KafkaListenerPreFilter(groupId = "test-consumer")
      public class VersionFilter implements Predicate<ConsumerRecord<Bytes, Bytes>> {
      
          Boolean apply(ConsumerRecord<Bytes, Bytes> consumerRecord) {
              return true
          }
      }
```

##### Producer
```java
      @Autowired
      private KafkaSender<String, DTO> kafkaSender;
```

##### Limitations
The current implementation supports only keys as strings and message bodies as JSON. 
It will use the ObjectMapper defined in the spring context  

### Manual
```java
      public void createConsumer() {
              final KafkaProperties.KafkaConsumerProperties kafkaConsumerProperties = new KafkaProperties.KafkaConsumerProperties();
              kafkaConsumerProperties.setGroupId("test-consumer");
              kafkaConsumerProperties.setAutoOffsetReset("earliest");
      
              final KafkaProperties.KafkaProducerProperties kafkaProducerProperties = new KafkaProperties.KafkaProducerProperties();
              kafkaProducerProperties.setMaxInFlight(10);
      
              final KafkaProperties kafkaProperties = new KafkaProperties();
              kafkaProperties.setBootstrapServers("localhost:9092");
              kafkaProperties.setConsumer(kafkaConsumerProperties);
              kafkaProperties.setProducer(kafkaProducerProperties);
      
              final KafkaListenerMeta<? extends String, ? extends String> kafkaListenerMeta = new KafkaListenerMeta(
                      message -> {
                          // Handle
                          return Completable.complete();
                      },
                      ImmutableList.of("topic1"),
                      String.class,
                      String.class,
                      new StringDeserializer(),
                      new StringDeserializer(),
                      Predicates.alwaysTrue(),
                      Predicates.alwaysTrue()
              );
      
              final KafkaConsumer kafkaConsumer = new KafkaConsumer(kafkaProperties, ImmutableList.of(kafkaListenerMeta));
              kafkaConsumer.start();
          }
```

## License
Apache License, Version 2.0

