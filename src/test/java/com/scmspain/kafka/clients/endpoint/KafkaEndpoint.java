package com.scmspain.kafka.clients.endpoint;

import com.google.inject.Inject;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.protocol.http.server.HttpServerRequest;
import io.reactivex.netty.protocol.http.server.HttpServerResponse;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rx.Observable;
import scmspain.karyon.restrouter.annotation.Endpoint;
import scmspain.karyon.restrouter.annotation.Path;
import javax.ws.rs.HttpMethod;

@Endpoint
public class KafkaEndpoint {


  private KafkaConsumer<String, String> consumer;
  private KafkaProducer<String, String> producer;
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaEndpoint.class);

  @Inject
  public KafkaEndpoint(KafkaProducer producer, KafkaConsumer consumer) {
    this.producer = producer;
    this.consumer = consumer;
  }


  @Path(value = "/consumer_test", method = HttpMethod.GET)
  public Observable<Void> getMessageFromKafka(HttpServerResponse<ByteBuf> response) {




    return Observable.empty();

  }

  @Path(value = "/producer_test", method = HttpMethod.GET)
  public Observable<Void> postMessageToKafka(HttpServerRequest<ByteBuf> request,
                                             HttpServerResponse<ByteBuf> response){
    String topic = "middleware_campaign_manager_test";
    String key = "config_created";
    String value ="Lalalla";
    ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, key, value);
    LOGGER.debug(String.format("Sending event '%s' with values: %s", key, value));
    for (int i=0;i<100000;i++){
      try {
        producer.send(producerRecord).get();
        LOGGER.debug(String.format("Sending event '%s' with values: %s", key, value));
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
