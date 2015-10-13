package com.scmspain.kafka.clients;

import com.google.inject.AbstractModule;
import com.scmspain.kafka.clients.provider.ConsumerProvider;
import com.scmspain.kafka.clients.provider.ProducerProvider;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public class KafkaClientsModule extends AbstractModule{

  @Override
  protected void configure() {

    bind(KafkaConsumer.class).toProvider(ConsumerProvider.class).asEagerSingleton();
    bind(KafkaProducer.class).toProvider(ProducerProvider.class).asEagerSingleton();

  }
}
