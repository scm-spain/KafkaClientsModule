package com.scmspain.kafka.clients.consumer;

import com.scmspain.kafka.clients.annotation.Consumer;
import com.scmspain.kafka.clients.annotation.Topic;
import kafka.message.MessageAndMetadata;
import rx.Observable;

@Consumer
public class KafkaConsumer implements ConsumerInterface<MessageAndMetadata<byte[], byte[]>> {


  public KafkaConsumer() {

  }

  @Override
  @Topic(value = "middleware_campaign_manager_test", groupId = "forlayo")
  public Observable<MessageAndMetadata<byte[], byte[]>> consume(Observable<MessageAndMetadata<byte[], byte[]>> message) {
    return message.doOnNext(data -> System.out.println(data.toString()+"***** from KafkaConsumer1"));
  }
}
