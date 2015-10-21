package com.scmspain.kafka.clients.consumer;

import com.google.common.base.Predicates;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.config.ConfigurationManager;
import com.scmspain.kafka.clients.annotation.Consumer;
import com.scmspain.kafka.clients.annotation.Topic;
import com.scmspain.kafka.clients.core.ResourceLoader;
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
    this.process();
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
        .forEach(method -> {
          String topic = method.getAnnotation(Topic.class).value();
          String groupId = method.getAnnotation(Topic.class).groupId();
          ConsumerConnector consumer = consumerConnectorBuilder.addGroupId(groupId).build();
          ObservableConsumer rxConsumer = new ObservableConsumer(consumer, topic);
          try {
            Observable resultObservable =(Observable) method.invoke(injector.getInstance(method.getDeclaringClass()), rxConsumer.toObservable());
            resultObservable.subscribe();

          } catch (Exception e) {
            throw new RuntimeException("Exception invoking method " + method.toString(), e);
          }
        })
    ;
  }
}
