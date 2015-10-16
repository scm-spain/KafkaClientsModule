package com.scmspain.kafka.clients.endpoint;

import com.google.inject.Inject;
import com.scmspain.kafka.clients.consumer.ObservableConsumer;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.ws.rs.HttpMethod;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import scmspain.karyon.restrouter.annotation.Endpoint;
import scmspain.karyon.restrouter.annotation.Path;

@Endpoint
public class KafkaEndpoint {

  private KafkaProducer<String, String> producer;
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEndpoint.class);

  @Inject
  public KafkaEndpoint(KafkaProducer producer) {
    this.producer = producer;
  }


  @Path(value = "/producer_test", method = HttpMethod.GET)
  public Observable<Void> postMessageToKafka(HttpServerRequest<ByteBuf> request,
                                             HttpServerResponse<ByteBuf> response){
    String topic = "middleware_campaign_manager_test";
    String value ="Lalalla";
    ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, value);
    for (int i=0;i<10000;i++){
      try {
        producer.send(producerRecord).get();
      } catch (InterruptedException e) {
        System.out.println(e.getMessage());
      } catch (ExecutionException e) {
        System.out.println(e.getMessage());
      }

    }

    producer.close();
    return response.writeStringAndFlush("forlayo");
  }

}

