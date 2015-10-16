package com.scmspain.kafka.clients;

import com.google.common.base.Predicates;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.netflix.config.ConfigurationManager;
import com.scmspain.kafka.clients.annotation.Consumer;
import com.scmspain.kafka.clients.annotation.Topic;
import com.scmspain.kafka.clients.consumer.ObservableConsumer;
import com.scmspain.kafka.clients.core.ResourceLoader;
import com.scmspain.kafka.clients.provider.ConsumerProvider;
import com.scmspain.kafka.clients.provider.ProducerProvider;
import java.lang.reflect.Modifier;
import java.util.Set;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.producer.KafkaProducer;
import rx.Observable;

import static org.reflections.ReflectionUtils.getAllMethods;
import static org.reflections.ReflectionUtils.withAnnotation;
import static org.reflections.ReflectionUtils.withModifier;
import static org.reflections.ReflectionUtils.withReturnType;

public class KafkaClientsModule extends AbstractModule{

  public static final String BASE_PACKAGE_PROPERTY = "com.scmspain.karyon.kafka.consumer.packages";
  private ResourceLoader resourceLoader;

  @Inject
  public KafkaClientsModule(Injector injector,ResourceLoader resourceLoader) {

    String basePackage = ConfigurationManager.getConfigInstance().getString(BASE_PACKAGE_PROPERTY);
    Set<Class<?>> annotatedTypes = resourceLoader.find(basePackage, Consumer.class);

    annotatedTypes.stream()
        .flatMap(klass ->
            getAllMethods(klass,
                Predicates.and(withModifier(Modifier.PUBLIC), withAnnotation(Topic.class)),
                withReturnType(Observable.class)
            ).stream())
        .forEach(method -> {
          String topic = method.getAnnotation(Topic.class).value();
          String groupId = method.getAnnotation(Topic.class).groupId();
          ConsumerProvider provider = new ConsumerProvider(groupId);
          ConsumerConnector consumer = provider.get();
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

  @Override
  protected void configure() {
    bind(KafkaProducer.class).toProvider(ProducerProvider.class).asEagerSingleton();
  }
}
