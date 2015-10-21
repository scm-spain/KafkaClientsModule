package com.scmspain.kafka.clients.consumer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import rx.Observable;
import rx.schedulers.Schedulers;

public class ObservableConsumer {

  private final ConsumerConnector consumer;
  private final String topic;

  public ObservableConsumer(ConsumerConnector consumer, String topic) {

    this.consumer = consumer;
    this.topic = topic;
  }

  public Observable<MessageAndMetadata<byte[], byte[]>> toObservable() {
    Map<String, Integer> topicCount = new HashMap<>();
    topicCount.put(topic, 4);

    Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer.createMessageStreams(topicCount);
    List<KafkaStream<byte[], byte[]>> streams = consumerStreams.get(topic);
    return Observable.from(streams)
        .flatMap(stream -> {
              return Observable.from((Iterable<MessageAndMetadata<byte[], byte[]>>) stream.toIterable())
                  .doOnNext(messageAndMetadata -> System.out.println(Thread.currentThread().getName()))
                  .subscribeOn(Schedulers.io());
            }
        )

        .doOnCompleted(() -> {
          if (consumer != null) {
            consumer.shutdown();
          }
        });

  }


}