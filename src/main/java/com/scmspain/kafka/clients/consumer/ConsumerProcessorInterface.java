package com.scmspain.kafka.clients.consumer;

import javax.annotation.PostConstruct;

public interface ConsumerProcessorInterface {
  @PostConstruct
  public void process();
}
