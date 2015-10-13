package com.scmspain.kafka.clients.provider;

import com.google.inject.Provider;
import com.netflix.config.ConfigurationManager;
import java.util.Iterator;
import java.util.Properties;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerProvider implements Provider<KafkaConsumer> {

  private final KafkaConsumer kafkaConsumer;

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerProvider.class);


  public ConsumerProvider(){
    this(ConfigurationManager.getConfigInstance().subset("kafka.consumer"));
  }

  public ConsumerProvider(Configuration configuration){
    Properties kafkaConsumerProperties = new Properties();

    for (Iterator<String> keys = configuration.getKeys(); keys.hasNext(); ){
      String key = keys.next();
      Object value = configuration.getProperty(key);
      kafkaConsumerProperties.put(key, value);
    }

    LOGGER.info("Kafka consumer loaded properties: " + kafkaConsumerProperties.toString());
    kafkaConsumer = new KafkaConsumer(kafkaConsumerProperties);

  }

  @Override
  public KafkaConsumer get() {
    return kafkaConsumer;
  }
}
