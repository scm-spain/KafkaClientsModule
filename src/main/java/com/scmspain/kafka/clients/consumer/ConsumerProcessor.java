package com.scmspain.kafka.clients.consumer;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Injector;

import com.netflix.config.ConfigurationManager;
import com.scmspain.kafka.clients.annotation.Consumer;
import com.scmspain.kafka.clients.core.ResourceLoader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import kafka.message.MessageAndMetadata;
import rx.Observer;

public class ConsumerProcessor {

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

  public List<ConsumerExecutor> getConsumers() {
    String basePackage = ConfigurationManager.getConfigInstance().getString(BASE_PACKAGE_PROPERTY);
    Set<Class<?>> annotatedTypes = resourceLoader.find(basePackage, Consumer.class);

    Preconditions.checkArgument(
        annotatedTypes.stream().allMatch(Observer.class::isAssignableFrom),
        "All Consumer should implement Observer rx class");

    return annotatedTypes.stream()
        .map(klass -> {
          Consumer consumerAnnotation = klass.getAnnotation(Consumer.class);
          String topic = consumerAnnotation.topic();
          String groupId = consumerAnnotation.groupId();
          int streams = consumerAnnotation.streams();

          //noinspection unchecked
          return new ConsumerExecutor(
              topic,
              groupId,
              streams,
              (Observer<MessageAndMetadata<byte[], byte[]>>)injector.getInstance(klass),
              klass,
              consumerConnectorBuilder
          );
        })
        .collect(Collectors.toList());
  }
}
