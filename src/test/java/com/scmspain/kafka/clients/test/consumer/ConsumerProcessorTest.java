package com.scmspain.kafka.clients.test.consumer;

import com.google.common.collect.Sets;
import com.google.inject.Injector;

import com.scmspain.kafka.clients.annotation.Consumer;
import com.scmspain.kafka.clients.consumer.ConsumerConnectorBuilder;
import com.scmspain.kafka.clients.consumer.ConsumerEngine;
import com.scmspain.kafka.clients.consumer.ConsumerProcessor;
import com.scmspain.kafka.clients.core.ResourceLoader;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mock;

import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndMetadata;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import rx.Observer;
import rx.Subscriber;
import rx.observers.TestObserver;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
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

//  @ClassRule
//  public static KafkaJunitRule kafkaRule = new KafkaJunitRule();

  private ConsumerConfig consumerConfig;

  private void consumerConfig() {
    Properties props = new Properties();
    props.put("zookeeper.connect", "192.168.99.100:2181");
    props.put("group.id", "ConsumerDoOnNext-groupId");
    props.put("zookeeper.session.timeout.ms", "400");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");

    consumerConfig = new ConsumerConfig(props);
  }

  @Before
  public void setUp() {
    initMocks(this);
    consumerConfig();
  }

  @Test
  @Ignore
  public void testProcess() throws Exception {
    given(resourceLoader.find(any(),any())).willReturn(Sets.newHashSet(TestConsumer.class));
    given(consumerConnectorBuilder.addGroupId(any())).willReturn(consumerConnectorBuilder);
    given(consumerConnectorBuilder.build()).willReturn(consumerConnector);
    given(consumerConnector.createMessageStreams(any(HashMap.class))).willReturn(new HashMap<String,List<KafkaStream<byte[], byte[]>>>());
    TestConsumer testConsumer = new TestConsumer();
    given(injector.getInstance(TestConsumer.class)).willReturn(testConsumer);
    ConsumerProcessor consumerProcessor = new ConsumerProcessor(injector,resourceLoader,consumerConnectorBuilder);
    ConsumerEngine consumerEngine = new ConsumerEngine(consumerProcessor);
    consumerEngine.startAll();

    assertFalse(testConsumer.isUnsubscribed());
  }

  @Test
  public void givenSomeMessagesWhenConsumerStartsItShouldConsumeAll() throws InterruptedException {
    // Given
    ConsumeAllConsumer consumer = new ConsumeAllConsumer(3);
    produceStreamOnTopic("ConsumeAllConsumer-topic", "1", "2", "3");
    configureConsumer(consumer);

    // When
    ConsumerProcessor consumerProcessor = new ConsumerProcessor(injector, resourceLoader, consumerConnectorBuilder);
    ConsumerEngine consumerEngine = new ConsumerEngine(consumerProcessor);
    consumerEngine.prepareConsumers();
    consumerEngine.startAll();

    // Then
    assertTrue(consumer.awaitCount(5, TimeUnit.SECONDS));
    consumerEngine.stop(ConsumeAllConsumer.class);
  }

  @Test
  public void givenSomeMessagesWhenConsumerStartsAndThenStoppedItShouldStopConsuming() throws Exception {
    produceStreamOnTopic("ConsumerDoOnNext-topic", "1", "2", "3");

    ConsumerProcessor consumerProcessor = new ConsumerProcessor(injector, resourceLoader, consumerConnectorBuilder);
    ConsumerEngine consumerEngine = new ConsumerEngine(consumerProcessor);

    ConsumerDoOnNext consumer = new ConsumerDoOnNext(1, msg -> {
      System.out.println("new String(msg) = " + new String(msg));
      consumerEngine.stopAll();
    });
    configureConsumer(consumer);

    consumerEngine.prepareConsumers();
    consumerEngine.startAll();

    assertTrue(consumer.awaitCount(5, TimeUnit.SECONDS));

    consumer.resetCountDown(1);
    consumerEngine.start(ConsumerDoOnNext.class);
    assertTrue(consumer.awaitCount(5, TimeUnit.SECONDS));

    consumer.resetCountDown(1);
    consumerEngine.start(ConsumerDoOnNext.class);
    assertTrue(consumer.awaitCount(5, TimeUnit.SECONDS));

    assertThat(consumer.getOnNextEvents().size(), is(3));
  }

  @Test(expected = UnsupportedOperationException.class)
  public void givenASubscriberConsumerWhenStopAndStartItShouldThrowAnUnsupportedOperation() {
    ConsumerProcessor consumerProcessor = new ConsumerProcessor(injector, resourceLoader, consumerConnectorBuilder);
    SubscriberConsumer consumer = new SubscriberConsumer();
    configureConsumer(consumer);

    ConsumerEngine consumerEngine = new ConsumerEngine(consumerProcessor);

    consumerEngine.prepareConsumers();
    consumerEngine.startAll();
    consumerEngine.stopAll();
    consumerEngine.startAll();
  }

  private void configureConsumer(Observer<MessageAndMetadata<byte[], byte[]>> consumer) {
    given(consumerConnectorBuilder.addGroupId(any())).willReturn(consumerConnectorBuilder);
    given(consumerConnectorBuilder.build()).willAnswer(invocationOnMock -> kafka.consumer.Consumer.createJavaConsumerConnector(consumerConfig));
    given(resourceLoader.find(any(),any())).willReturn(Sets.newHashSet(consumer.getClass()));

    // Due to generics madness we have to invert the
    willReturn(consumer).given(injector).getInstance(consumer.getClass());
  }

  private void produceStreamOnTopic(String topic, String... messages) {
    Properties props = new Properties();

    props.put("metadata.broker.list", "192.168.99.100:9092");
    props.put("producer.type", "sync");
    props.put("request.required.acks", "1");

    ProducerConfig conf = new ProducerConfig(props);

    Producer<byte[], byte[]> producer = new Producer<>(conf);
    for(String message: messages) {
      producer.send(new KeyedMessage<>(topic, "same_key".getBytes(), message.getBytes()));
    }
    producer.close();
  }

  @Consumer(topic = "SubscriberConsumer-topic", groupId = "SubscriberConsumer-groupId",streams = 1)
  private static class SubscriberConsumer extends Subscriber<MessageAndMetadata<byte[], byte[]>> {

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

  @Consumer(topic = "ConsumerDoOnNext-topic", groupId = "ConsumerDoOnNext-groupId",streams = 1)
  private static class ConsumerDoOnNext extends CountDownConsumer {
    private java.util.function.Consumer<byte[]> onNext;

    public ConsumerDoOnNext(int count, java.util.function.Consumer<byte[]> onNext) {
      super(count);
      this.onNext = onNext;
    }

    @Override
    public void onNext(MessageAndMetadata<byte[], byte[]> messageAndMetadata) {
      onNext.accept(messageAndMetadata.message());
      super.onNext(messageAndMetadata);
    }
  }

  @Consumer(topic = "ConsumeAllConsumer-topic", groupId = "ConsumeAllConsumer-groupId", streams = 1)
  private class ConsumeAllConsumer extends CountDownConsumer {
    public ConsumeAllConsumer(int count) {
      super(count);
    }
  }
  private static class CountDownConsumer extends TestObserver<MessageAndMetadata<byte[], byte[]>> {
    private CountDownLatch countDownLatch;

    public CountDownConsumer(int count) {
      this.countDownLatch = new CountDownLatch(count);
    }

    public boolean awaitCount(int timeout, TimeUnit unit) throws InterruptedException {
      return countDownLatch.await(timeout, unit);
    }

    public CountDownLatch getCountDownLatch() {
      return countDownLatch;
    }

    public void resetCountDown(int count) {
      countDownLatch = new CountDownLatch(count);
    }

    @Override
    public void onNext(MessageAndMetadata<byte[], byte[]> messageAndMetadata) {
      super.onNext(messageAndMetadata);
      countDownLatch.countDown();
    }
  }

  @Consumer(topic = "pepe",groupId = "forlayo",streams = 2)
  private class TestConsumer extends DoNothingConsumer {

  }

  private class DoNothingConsumer extends Subscriber<MessageAndMetadata<byte[], byte[]>> {

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