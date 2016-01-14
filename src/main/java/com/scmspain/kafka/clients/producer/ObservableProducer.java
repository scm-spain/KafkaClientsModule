package com.scmspain.kafka.clients.producer;

import com.google.inject.Inject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;

public class ObservableProducer {

  private KafkaProducer<String, String> producer;
  private static final Logger LOGGER = LoggerFactory.getLogger(ObservableProducer.class);

  @Inject
  public ObservableProducer(KafkaProducer<String, String> producer) {
    this.producer = producer;
  }

  public Observable<RecordMetadata> send(ProducerRecord record){

    return Observable.from(producer.send(record))
        .doOnError(error -> LOGGER.error(error.getMessage()))
        ;

  }
}
