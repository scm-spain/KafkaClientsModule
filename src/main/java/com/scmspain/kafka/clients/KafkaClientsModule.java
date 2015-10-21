package com.scmspain.kafka.clients;

import com.google.inject.AbstractModule;
import com.scmspain.kafka.clients.consumer.ConsumerConnectorBuilder;
import com.scmspain.kafka.clients.consumer.ConsumerProcessor;
import com.scmspain.kafka.clients.consumer.ConsumerProcessorInterface;
import com.scmspain.kafka.clients.provider.ProducerProvider;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaClientsModule extends AbstractModule{


  @Override
  protected void configure() {
    bind(KafkaProducer.class).toProvider(ProducerProvider.class).asEagerSingleton();
    bind(ConsumerProcessorInterface.class).to(ConsumerProcessor.class).asEagerSingleton();
    bind(ConsumerConnectorBuilder.class);
  }
}
