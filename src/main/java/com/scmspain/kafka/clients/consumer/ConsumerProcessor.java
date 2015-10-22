package com.scmspain.kafka.clients.consumer;

import com.google.common.base.Predicates;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.config.ConfigurationManager;
import com.scmspain.kafka.clients.annotation.Consumer;
import com.scmspain.kafka.clients.annotation.Topic;
import com.scmspain.kafka.clients.core.ResourceLoader;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Set;
import kafka.javaapi.consumer.ConsumerConnector;
import rx.Observable;

import static org.reflections.ReflectionUtils.getAllMethods;
import static org.reflections.ReflectionUtils.withAnnotation;
import static org.reflections.ReflectionUtils.withModifier;
import static org.reflections.ReflectionUtils.withName;
import static org.reflections.ReflectionUtils.withReturnType;

public class ConsumerProcessor implements ConsumerProcessorInterface{

  private Injector injector;
  private ResourceLoader resourceLoader;
  private ConsumerConnectorBuilder consumerConnectorBuilder;

  public static final String BASE_PACKAGE_PROPERTY = "com.scmspain.karyon.kafka.consumer.packages";
  private static final String CONSUMER_METHOD = "consume";

  @Inject
  public ConsumerProcessor(Injector injector, ResourceLoader resourceLoader, ConsumerConnectorBuilder consumerConnectorBuilder) {
    this.injector = injector;
    this.resourceLoader = resourceLoader;
    this.consumerConnectorBuilder = consumerConnectorBuilder;
  }

  @Override
  public void process() {
    String basePackage = ConfigurationManager.getConfigInstance().getString(BASE_PACKAGE_PROPERTY);
    Set<Class<?>> annotatedTypes = resourceLoader.find(basePackage, Consumer.class);

    annotatedTypes.stream()
        .flatMap(klass ->
            getAllMethods(klass,
                Predicates.and(withModifier(Modifier.PUBLIC), withName(CONSUMER_METHOD), withAnnotation(Topic.class)),
                withReturnType(Observable.class)
            ).stream())
        .map(method -> {
          Topic topicAnnotation = method.getAnnotation(Topic.class);
          String topic = topicAnnotation.value();
          String groupId = topicAnnotation.groupId();
          int streams = topicAnnotation.streams();
          return new ConsumerExecutor(topic,groupId,streams,method);
        })
        .forEach(consumerExecutor -> {
          ConsumerConnector consumer = consumerConnectorBuilder.addGroupId(consumerExecutor.groupId).build();
          ObservableConsumer rxConsumer = new ObservableConsumer(consumer, consumerExecutor.topic, consumerExecutor.streams);
          try {
            Observable resultObservable = (Observable) consumerExecutor.method.invoke(injector.getInstance(consumerExecutor.method.getDeclaringClass()), rxConsumer.toObservable());
            resultObservable.subscribe();

          } catch (Exception e) {
            throw new RuntimeException("Exception invoking method " + consumerExecutor.method.toString(), e);
          }
        })
    ;
  }

  private class ConsumerExecutor {
    public String topic;
    public String groupId;
    public int streams;
    public Method method;

    public ConsumerExecutor(String topic, String groupid, int streams, Method method) {
      this.topic = topic;
      this.groupId = groupid;
      this.streams = streams;
      this.method = method;
    }
  }


}
