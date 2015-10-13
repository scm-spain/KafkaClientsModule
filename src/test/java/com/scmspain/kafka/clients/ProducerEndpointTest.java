package com.scmspain.kafka.clients;

import com.scmspain.kafka.clients.core.KaryonControllerTest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.reactivex.netty.protocol.http.client.HttpClientRequest;
import io.reactivex.netty.protocol.http.client.HttpClientResponse;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

public class ProducerEndpointTest extends KaryonControllerTest {
  @Test
  public void itShouldUpdateAnExistingCampaign() throws Exception {

    final String body = createHttpClient()
        .submit(
            HttpClientRequest.createGet("/producer_test")
        )
        .doOnNext(response -> Assert.assertEquals(HttpResponseStatus.OK, response.getStatus()))
        .flatMap(HttpClientResponse::getContent)
        .map(this::asString)
        .timeout(100, TimeUnit.SECONDS)
        .toBlocking().single();

    Assert.assertEquals("forlayo", body);
  }
}
