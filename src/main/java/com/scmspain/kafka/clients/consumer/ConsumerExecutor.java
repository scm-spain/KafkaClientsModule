package com.scmspain.kafka.clients.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import rx.Observer;
import rx.Subscriber;
import rx.Subscription;

class ConsumerExecutor {
  private String topic;
  private String groupId;
  private int streams;
  private Observer<MessageAndMetadata<byte[], byte[]>> observer;
  private Subscription subscription;
  private ConsumerConnectorBuilder consumerConnectorBuilder;
  private Class consumerClass;
  private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerExecutor.class);

  ConsumerExecutor(String topic, String groupId, int streams, Observer<MessageAndMetadata<byte[], byte[]>> observer, Class consumerClass, ConsumerConnectorBuilder consumerConnectorBuilder) {
    this.topic = topic;
    this.groupId = groupId;
    this.streams = streams;
    this.observer = observer;
    this.consumerConnectorBuilder = consumerConnectorBuilder;
    this.consumerClass = consumerClass;
    this.subscription = null;
  }

  void stop() {
    if (isRunning()) {
      subscription.unsubscribe();
    }
  }

  void start() {
    if (!isRunning()) {
      if(subscription != null && Subscriber.class.isAssignableFrom(consumerClass)) {
        throw new UnsupportedOperationException(
            String.format("Cannot resume the consumer %s as a Subscriber, don't inherit from Subscriber on Consumer if you want to use this feature, use Observer instead", consumerClass.getName())
        );
      }
      ConsumerConnector consumerConnector = consumerConnectorBuilder.addGroupId(groupId).build();
      ObservableConsumer rxConsumer = new ObservableConsumer(consumerConnector, topic, streams);

      subscription = rxConsumer.toObservable().subscribe(observer);

    }
  }

  Class getConsumerClass() {
    return consumerClass;
  }


  boolean isRunning() {
    return subscription != null && !subscription.isUnsubscribed();
  }
}
