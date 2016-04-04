package com.scmspain.kafka.clients.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import rx.Observer;

public class ConsumerEngine {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerEngine.class);
  private ConsumerProcessor consumerProcessor;
  private Map<Class, ConsumerExecutor> consumerExecutors;

  @Inject
  public ConsumerEngine(ConsumerProcessor consumerProcessor) {
    this.consumerProcessor = consumerProcessor;
    this.consumerExecutors = Collections.emptyMap();
  }

  @PostConstruct
  public void prepareConsumers() {
    this.consumerExecutors = consumerProcessor.getConsumers().stream()
        .collect(Collectors.toMap(ConsumerExecutor::getConsumerClass, Function.identity()));

    startAll();
  }

  public void stop(Class<? extends Observer> clazz) {
    Optional.ofNullable(consumerExecutors.get(clazz))
        .ifPresent(ConsumerExecutor::stop);
  }

  public void start(Class<? extends Observer> clazz) {
    Optional.ofNullable(consumerExecutors.get(clazz))
        .ifPresent(ConsumerExecutor::start);
  }

  public boolean isRunning(Class<? extends Observer> clazz) {
    return Optional.ofNullable(consumerExecutors.get(clazz))
        .map(ConsumerExecutor::isRunning)
        .orElse(false);
  }

  public void startAll() {
    consumerExecutors.values()
        .forEach(ConsumerExecutor::start);
  }

  public void stopAll() {
    consumerExecutors.values()
        .forEach(ConsumerExecutor::stop);
  }

  @PreDestroy
  private void shutdownOnPreDestroy() {
    try {
      LOGGER.info("Shutdown all consumer instances...");
      stopAll();
      LOGGER.info("Shutdown consumers complete");

    } catch(Throwable t) {
      LOGGER.error("Consumer shutdown error", t);
    }
  }

}
