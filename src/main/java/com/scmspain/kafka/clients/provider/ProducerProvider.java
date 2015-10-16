package com.scmspain.kafka.clients.provider;

import com.google.inject.Provider;
import com.netflix.config.ConfigurationManager;
import java.util.Iterator;
import java.util.Properties;
import java.util.stream.StreamSupport;
import org.apache.commons.configuration.Configuration;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerProvider implements Provider<KafkaProducer>{

  private final KafkaProducer kafkaProducer;

  private static final Logger LOGGER = LoggerFactory.getLogger(ProducerProvider.class);

  public ProducerProvider(){
    this(ConfigurationManager.getConfigInstance().subset("kafka.producer"));
  }

  public ProducerProvider(Configuration configuration){
    Properties kafkaProducerProperties = new Properties();

    Iterable<String> iterable = configuration::getKeys;
    StreamSupport.stream(iterable.spliterator(), false)
        .forEach(key -> kafkaProducerProperties.put(key, configuration.getProperty(key)));

    LOGGER.info("Kafka producer loaded properties: " + kafkaProducerProperties.toString());
    kafkaProducer = new KafkaProducer(kafkaProducerProperties);

  }

  @Override
  public KafkaProducer get() {
    return kafkaProducer;
  }
}
