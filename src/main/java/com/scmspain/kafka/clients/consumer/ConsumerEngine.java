package com.scmspain.kafka.clients.consumer;

import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import rx.Observer;

public class ConsumerEngine {
  private ConsumerProcessor consumerProcessor;
  private Map<Class, ConsumerExecutor> consumerExecutors;

  @Inject
  public ConsumerEngine(ConsumerProcessor consumerProcessor) {
    this.consumerProcessor = consumerProcessor;

  }

  @PostConstruct
  public void prepareConsumers() {
    this.consumerExecutors = consumerProcessor.getConsumers().stream()
        .collect(Collectors.toMap(ConsumerExecutor::getConsumerClass, Function.identity()));

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

}
