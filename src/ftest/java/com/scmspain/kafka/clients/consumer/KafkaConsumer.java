package com.scmspain.kafka.clients.consumer;

import com.scmspain.kafka.clients.annotation.Consumer;
import kafka.message.MessageAndMetadata;
import rx.Subscriber;

@Consumer(topic = "middleware_campaign_manager_test", groupId = "forlayo", streams = 2)
public class KafkaConsumer extends Subscriber<MessageAndMetadata<byte[], byte[]>> {


  public KafkaConsumer() {

  }

  @Override
  public void onCompleted() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onError(Throwable e) {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  @Override
  public void onNext(MessageAndMetadata<byte[], byte[]> messageAndMetadata) {
    System.out.println(messageAndMetadata.message() + "***** from KafkaConsumer1");
  }
}
