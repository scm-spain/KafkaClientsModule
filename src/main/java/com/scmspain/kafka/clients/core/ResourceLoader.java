package com.scmspain.kafka.clients.core;

import com.google.inject.Singleton;
import java.util.Set;
import org.reflections.Reflections;

@Singleton
public class ResourceLoader {

  public Set<Class<?>> find(String packageName, Class annotation) {
    Reflections reflections = new Reflections(packageName);
    return reflections.getTypesAnnotatedWith(annotation);
  }
}
