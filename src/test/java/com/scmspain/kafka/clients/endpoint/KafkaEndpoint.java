package com.scmspain.kafka.clients.endpoint;

import com.google.inject.Inject;
import com.scmspain.kafka.clients.consumer.SimpleHLConsumer;
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


    SimpleHLConsumer simpleHLConsumer = new SimpleHLConsumer("192.168.59.103:2181", "default", "middleware_campaign_manager_test");
    simpleHLConsumer.testConsumer();

    return Observable.empty();

  }

  @Path(value = "/producer_test", method = HttpMethod.GET)
  public Observable<Void> postMessageToKafka(HttpServerRequest<ByteBuf> request,
                                             HttpServerResponse<ByteBuf> response){
    String topic = "middleware_campaign_manager_test";
    String value ="Lalalla";
    ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, value);
    for (int i=0;i<1000;i++){
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

  private Map<TopicPartition, Long> process(Map<String, ConsumerRecords<String,String>> records) {
    Map<TopicPartition, Long> processedOffsets = new HashMap<TopicPartition, Long>();
    for(Map.Entry<String, ConsumerRecords<String,String>> recordMetadata : records.entrySet()) {
      List<ConsumerRecord<String, String>> recordsPerTopic = recordMetadata.getValue().records();
      for(int i = 0;i < recordsPerTopic.size();i++) {
        ConsumerRecord<String, String> record = recordsPerTopic.get(i);
        System.out.println(record.toString());
        try {
          LOGGER.info(record.value());
          processedOffsets.put(record.topicAndPartition(), record.offset());
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    return processedOffsets;
  }
}

