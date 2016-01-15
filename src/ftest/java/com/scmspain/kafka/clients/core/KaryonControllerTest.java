package com.scmspain.kafka.clients.core;

import com.netflix.governator.guice.BootstrapModule;
import io.netty.buffer.ByteBuf;
import io.reactivex.netty.RxNetty;
import io.reactivex.netty.protocol.http.client.HttpClient;
import java.nio.charset.Charset;
import netflix.karyon.Karyon;
import netflix.karyon.KaryonServer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 */
abstract public class KaryonControllerTest {
  private static KaryonServer server;

  @BeforeClass
  public static void setUpBefore() throws Exception {
    server = Karyon.forApplication(AppServerTest.class, (BootstrapModule[]) null);
    server.start();
  }

  @AfterClass
  public static void tearDownAfter() throws Exception {
    if (server != null) {
      server.shutdown();
    }
  }

  protected HttpClient<ByteBuf, ByteBuf> createHttpClient() {
    return RxNetty.createHttpClient("localhost", AppServerTest.KaryonRestRouterModuleImpl.DEFAULT_PORT);
  }

  protected String asString(ByteBuf content) {
    return content.toString(Charset.defaultCharset());
  }
}
