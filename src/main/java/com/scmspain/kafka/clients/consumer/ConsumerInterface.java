package com.scmspain.kafka.clients.consumer;

import kafka.message.MessageAndMetadata;
import rx.Observable;

public interface ConsumerInterface<T> {

  public Observable<T> consume(Observable<MessageAndMetadata<byte[], byte[]>> message);

}
