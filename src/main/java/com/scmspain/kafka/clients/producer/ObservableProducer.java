package com.scmspain.kafka.clients.producer;

import com.google.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import rx.Observable;

public class ObservableProducer {

  private KafkaProducer<String, String> producer;

  @Inject
  public ObservableProducer(KafkaProducer producer) {
    this.producer = producer;
  }

  public Observable<RecordMetadata> send(ProducerRecord record){

    return Observable.from(producer.send(record));

  }
}
