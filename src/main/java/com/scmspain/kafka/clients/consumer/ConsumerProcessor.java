package com.scmspain.kafka.clients.consumer;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.config.ConfigurationManager;
import com.scmspain.kafka.clients.annotation.Consumer;
import com.scmspain.kafka.clients.core.ResourceLoader;
import java.util.Set;
import kafka.javaapi.consumer.ConsumerConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Subscriber;
import rx.Subscription;

public class ConsumerProcessor implements ConsumerProcessorInterface{

  private Injector injector;
  private ResourceLoader resourceLoader;
  private ConsumerConnectorBuilder consumerConnectorBuilder;
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerProcessor.class);

  public static final String BASE_PACKAGE_PROPERTY = "com.scmspain.karyon.kafka.consumer.packages";

  @Inject
  public ConsumerProcessor(Injector injector, ResourceLoader resourceLoader, ConsumerConnectorBuilder consumerConnectorBuilder) {
    this.injector = injector;
    this.resourceLoader = resourceLoader;
    this.consumerConnectorBuilder = consumerConnectorBuilder;
  }

  @Override
  public void process() {
    String basePackage = ConfigurationManager.getConfigInstance().getString(BASE_PACKAGE_PROPERTY);
    Set<Class<?>> annotatedTypes = resourceLoader.find(basePackage, Consumer.class);

    annotatedTypes.stream()
        .map(klass -> {
          Consumer consumerAnnotation = klass.getAnnotation(Consumer.class);
          String topic = consumerAnnotation.topic();
          String groupId = consumerAnnotation.groupId();
          int streams = consumerAnnotation.streams();
          return new ConsumerExecutor(topic, groupId, streams,klass);
        })
        .forEach(consumerExecutor -> {
          ConsumerConnector consumer = consumerConnectorBuilder.addGroupId(consumerExecutor.groupId).build();
          ObservableConsumer rxConsumer = new ObservableConsumer(consumer, consumerExecutor.topic, consumerExecutor.streams);
          try {
            Subscriber subscriber = (Subscriber)injector.getInstance(consumerExecutor.klass);
            Subscription subscription = rxConsumer.toObservable().subscribe(subscriber);
            subscriber.add(subscription);

          } catch (Exception e) {
            LOGGER.error(e.getMessage());
          }
        })
    ;
  }

  private class ConsumerExecutor {
    public String topic;
    public String groupId;
    public int streams;
    public Class<?> klass;

    public ConsumerExecutor(String topic, String groupid, int streams, Class<?> klass) {
      this.topic = topic;
      this.groupId = groupid;
      this.streams = streams;
      this.klass = klass;
    }
  }


}
