package com.scmspain.kafka.clients.core;


import com.google.inject.Singleton;
import com.netflix.config.DynamicPropertyFactory;
import com.netflix.governator.annotations.Modules;
import com.scmspain.kafka.clients.KafkaClientsModule;
import com.scmspain.kafka.clients.endpoint.KafkaEndpoint;
import netflix.karyon.KaryonBootstrap;
import netflix.karyon.archaius.ArchaiusBootstrap;
import scmspain.karyon.restrouter.KaryonRestRouterModule;
import scmspain.karyon.restrouter.serializer.Configuration;

@ArchaiusBootstrap()
@KaryonBootstrap(name = "AppServerTest")
@Singleton
@Modules(include = {
    AppServerTest.KaryonRestRouterModuleImpl.class,
    KafkaClientsModule.class
})

public interface AppServerTest {
  class KaryonRestRouterModuleImpl extends KaryonRestRouterModule {

    public static final int DEFAULT_PORT = 8000;
    public static final int DEFAULT_THREADS = 50;
    private final DynamicPropertyFactory properties = DynamicPropertyFactory.getInstance();

    @Override
    protected void configureServer() {

      int port = properties.getIntProperty("server.port", DEFAULT_PORT).get();
      int threads = properties.getIntProperty("server.threads", DEFAULT_THREADS).get();
      server().port(port).threadPoolSize(threads);

      this.setConfiguration(Configuration.builder()
              .defaultContentType("application/json")
              .addSerializer(new JsonSerializer())
              .build()
      );

    }

    @Override
    public void configure() {
      bind(KafkaEndpoint.class).asEagerSingleton();
      super.configure();
    }
  }
}
