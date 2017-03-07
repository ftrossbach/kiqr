package com.github.ftrossbach.kiqr.rest.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.Properties;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

/**
 * Created by ftr on 07/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class GenericHttpServerTest {


    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    @Test
    public void deploymentFailure(TestContext context){
        rule.vertx().deployVerticle(new HttpServer(new HttpServerOptions().setPort(5762), new DummyFailingVerticle()), context.asyncAssertFailure());
    }

    @Test
    public void builderWithConfig(){
        HttpServer.Builder builder = new HttpServer.Builder(new KStreamBuilder(), new Properties());

        AbstractVerticle serverVerticle = builder.withOptions(new HttpServerOptions().setPort(4711)).build();
        assertThat(serverVerticle, is(instanceOf(HttpServer.class)));

        HttpServer server = (HttpServer) serverVerticle;
        assertThat(server.serverOptions.getPort(), is(equalTo(4711)));

    }

    @Test
    public void builderWithPort(){
        HttpServer.Builder builder = new HttpServer.Builder(new KStreamBuilder(), new Properties());

        AbstractVerticle serverVerticle = builder.withPort(4711).build();
        assertThat(serverVerticle, is(instanceOf(HttpServer.class)));

        HttpServer server = (HttpServer) serverVerticle;
        assertThat(server.serverOptions.getPort(), is(equalTo(4711)));

    }
}
