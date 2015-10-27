# com.scmspain.kafka.clients.KafkaClientsModule
Karyon Guice Module for handle kafka consumers and producers in a friendly way


# Getting Started

Binaries and dependency information for Local Artifactory.

Example of Gradle:

```groovy
dependencies {
       compile 'com.scmspain.karyon:karyon-rest-router:1.1'   
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

### 4- Injecting producer in your classes

```java
private KafkaProducer<String, String> producer;

 @Inject
  public ExampleConstructor(KafkaProducer producer) {
    this.producer = producer;
  }

```

