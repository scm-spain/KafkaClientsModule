package com.scmspain.kafka.clients.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

public class ConsumerShutdownHook {
  private final ConsumerEngine consumerEngine;
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerShutdownHook.class);

  @Inject
  public ConsumerShutdownHook(ConsumerEngine consumerEngine) {
    this.consumerEngine = consumerEngine;
  }

  @PostConstruct
  public void addShutdownHook() {
    Thread shutdownThread = new Thread(() -> {
      LOGGER.info("Shutdown all consumer instances...");
      consumerEngine.stopAll();
      LOGGER.info("Shutdown consumers complete");
    });

    Runtime.getRuntime().addShutdownHook(shutdownThread);
  }


}
