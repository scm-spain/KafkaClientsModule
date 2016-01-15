package com.scmspain.kafka.clients.consumer;

import com.google.common.collect.Sets;
import com.google.inject.Injector;
import com.scmspain.kafka.clients.annotation.Consumer;
import com.scmspain.kafka.clients.consumer.ConsumerConnectorBuilder;
import com.scmspain.kafka.clients.consumer.ConsumerProcessor;
import com.scmspain.kafka.clients.core.ResourceLoader;
import java.util.HashMap;
import java.util.List;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import rx.Subscriber;

import static org.junit.Assert.assertFalse;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.MockitoAnnotations.initMocks;

public class ConsumerProcessorTest {

  @Mock
  private Injector injector;
  @Mock
  private ResourceLoader resourceLoader;
  @Mock
  private ConsumerConnectorBuilder consumerConnectorBuilder;
  @Mock
  private ConsumerConnector consumerConnector;

  @Before
  public void setUp() {
    initMocks(this);
  }

  @Test
  public void testProcess() throws Exception {


    given(resourceLoader.find(any(),any())).willReturn(Sets.newHashSet(TestConsumer.class));
    given(consumerConnectorBuilder.addGroupId(any())).willReturn(consumerConnectorBuilder);
    given(consumerConnectorBuilder.build()).willReturn(consumerConnector);
    given(consumerConnector.createMessageStreams(any(HashMap.class))).willReturn(new HashMap<String,List<KafkaStream<byte[], byte[]>>>());
    TestConsumer testConsumer = new TestConsumer();
    given(injector.getInstance(TestConsumer.class)).willReturn(testConsumer);
    ConsumerProcessor consumerProcessor = new ConsumerProcessor(injector,resourceLoader,consumerConnectorBuilder);
    consumerProcessor.process();

    assertFalse(testConsumer.isUnsubscribed());




  }



  @Consumer(topic = "pepe",groupId = "forlayo",streams = 2)
  private static class TestConsumer extends Subscriber<MessageAndMetadata<byte[], byte[]>> {

    @Override
    public void onCompleted() {

    }

    @Override
    public void onError(Throwable e) {

    }

    @Override
    public void onNext(MessageAndMetadata<byte[], byte[]> messageAndMetadata) {

    }
  }
}