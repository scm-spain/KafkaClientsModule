package com.scmspain.kafka.clients.consumer;

import com.netflix.config.ConfigurationManager;
import java.util.Properties;
import java.util.stream.StreamSupport;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.commons.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ConsumerConnectorBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerConnectorBuilder.class);
  private Properties props;

  public ConsumerConnectorBuilder() {

    Configuration configuration = ConfigurationManager.getConfigInstance().subset("kafka.consumer");
    this.props = new Properties();

    Iterable<String> iterable = configuration::getKeys;
    StreamSupport.stream(iterable.spliterator(), false)
        .forEach(key -> props.put(key, configuration.getProperty(key)));

  }

  public ConsumerConnectorBuilder addGroupId(String groupId) {
    props.put("group.id", groupId);
    return this;
  }

  public ConsumerConnector build() {
    LOGGER.info("Kafka consumer loaded properties: " + props.toString());
    return Consumer.createJavaConsumerConnector(new ConsumerConfig(props));
  }
}
