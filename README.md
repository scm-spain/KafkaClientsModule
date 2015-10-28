# com.scmspain.kafka.clients.KafkaClientsModule
Karyon Guice Module for handle kafka consumers and producers in a friendly way


# Getting Started

Binaries and dependency information for Maven Central.

Example of Gradle:

```groovy
dependencies {
       compile 'com.scmspain.karyon:karyon2-kafka:0.1.0'   
}
```


## Using the module in your server

### 1- Include the module in karyon bootstrap

```java
@Modules(include = {
    KafkaClientsModule.class,
    ...
})
```

### 3- Setting package name to find consumers

Set this property in your .properties files for example AppServer.properties

```
com.scmspain.karyon.kafka.consumer.packages = com.scmspain.kafka.clients.consumer
```

### 4- Configuring your consumers and producer with Archaius

You have all properties from kafka consumer and producer from the official docs availables in your archaius .properties files with a prefix for consumer and producer:
 
```
kafka.producer.bootstrap.servers=kafka.host:9092
kafka.producer.acks=1
kafka.producer.value.serializer=org.apache.kafka.common.serialization.StringSerializer
kafka.producer.key.serializer=org.apache.kafka.common.serialization.StringSerializer

kafka.consumer.zookeeper.connect=zookeper.host:2181
kafka.consumer.enable.auto.commit=true
kafka.consumer.auto.commit.interval.ms=10000
kafka.consumer.zookeeper.session.timeout.ms=500
kafka.consumer.zookeeper.sync.time.ms=250
kafka.consumer.partition.assignment.strategy=roundrobin

```
 
The group.id property from the consumer is setted by the Consumer class with the Annotation @Consumer.
Also with @Consumer you can set the topic name and the number of streams (number of threads to wakeup and consum messages)



### 5- Injecting producer in your classes and using it

```java
  private KafkaProducer<String, String> producer;

  @Inject
  public ExampleConstructor(KafkaProducer producer) {
    this.producer = producer;
  }
  
  
  @Path(value = "/producer_test", method = HttpMethod.GET)
  public Observable<Void> postMessageToKafka(HttpServerRequest<ByteBuf> request,
                                             HttpServerResponse<ByteBuf> response){
    String topic = example_topic_name";
    String value ="Message to be send!!";
    String key = "42"; 
    ProducerRecord<String,String> producerRecord;
    for (int i=0;i<10000;i++){
      try {
        producerRecord = new ProducerRecord<>(topic, value+"_"+i);
        producer.send(producerRecord).get();
      } catch (InterruptedException e) {
        System.out.println(e.getMessage());
      } catch (ExecutionException e) {
        System.out.println(e.getMessage());
      }

    }
    producer.close();
    return response.writeStringAndFlush("forlayo");

  

```

