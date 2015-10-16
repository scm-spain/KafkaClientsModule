package com.scmspain.kafka.clients.provider;

import com.google.inject.Singleton;
import com.netflix.config.ConfigurationManager;
import java.util.Properties;
import java.util.stream.StreamSupport;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
@Singleton
public class ConsumerProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerProvider.class);
  private final ConsumerConnector consumer;

  public ConsumerProvider(String groupId){
    Configuration configuration = ConfigurationManager.getConfigInstance().subset("kafka.consumer");
    configuration.addProperty("group.id", groupId);

    Properties props = new Properties();

    Iterable<String> iterable = configuration::getKeys;
    StreamSupport.stream(iterable.spliterator(), false)
        .forEach(key -> props.put(key, configuration.getProperty(key)));

    LOGGER.info("Kafka consumer loaded properties: " + props.toString());
    consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(props));

  }

  public ConsumerConnector get() {
    return consumer;
  }
}
