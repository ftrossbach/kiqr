package com.github.ftrossbach.kiqr.core;

import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.Properties;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;


import static org.mockito.Mockito.*;

/**
 * Created by ftr on 06/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class RuntimeVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Test
    public void successfulStart(TestContext context){

        KafkaStreams streamsMock = mock(KafkaStreams.class);
        KStreamBuilder builderMock = mock(KStreamBuilder.class);
        Properties props = new Properties();
        RuntimeVerticle verticleSpy = spy(new RuntimeVerticle(builderMock, props));

        doReturn(streamsMock).when(verticleSpy).createAndStartStream();

        rule.vertx().deployVerticle(verticleSpy, context.asyncAssertSuccess(handler -> {


            context.assertTrue(rule.vertx().deploymentIDs().size() > 0);
        }));

    }

    @Test
    public void successfulStartIncludingServer(TestContext context){

        KafkaStreams streamsMock = mock(KafkaStreams.class);
        KStreamBuilder builderMock = mock(KStreamBuilder.class);
        Properties props = new Properties();
        RuntimeVerticle verticleSpy = spy(new RuntimeVerticle(builderMock, props, new HttpServerOptions().setPort(29010)));

        doReturn(streamsMock).when(verticleSpy).createAndStartStream();

        rule.vertx().deployVerticle(verticleSpy, context.asyncAssertSuccess(handler -> {


            context.assertTrue(rule.vertx().deploymentIDs().size() > 0);
        }));

    }

    @Test
    public void builderApplicationId(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withApplicationId("test");
        RuntimeVerticle verticle = builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.APPLICATION_ID_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.APPLICATION_ID_CONFIG), is(equalTo("test")));

    }

    @Test
    public void builderBootstrapServer(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withBootstrapServers("localhost:123");
        RuntimeVerticle verticle = builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG), is(equalTo("localhost:123")));

    }

    @Test
    public void builderWithCaching(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withBuffering(42);
        RuntimeVerticle verticle = builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG), is(equalTo(42)));

    }

    @Test
    public void builderWithoutCaching(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withoutBuffering();
        RuntimeVerticle verticle = builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG), is(equalTo(0)));

    }

    @Test
    public void builderWithKeySerdeString(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withKeySerde(Serdes.String());
        RuntimeVerticle verticle = builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.KEY_SERDE_CLASS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.KEY_SERDE_CLASS_CONFIG), is(equalTo(Serdes.String().getClass().getName())));

    }

    @Test
    public void builderWithKeySerdeClass(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withKeySerde(Serdes.String().getClass());
        RuntimeVerticle verticle = builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.KEY_SERDE_CLASS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.KEY_SERDE_CLASS_CONFIG), is(equalTo(Serdes.String().getClass().getName())));

    }

    @Test
    public void builderWithValueSerdeString(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withValueSerde(Serdes.String());
        RuntimeVerticle verticle = builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.VALUE_SERDE_CLASS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.VALUE_SERDE_CLASS_CONFIG), is(equalTo(Serdes.String().getClass().getName())));

    }

    @Test
    public void builderWithValueSerdeClass(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withValueSerde(Serdes.String().getClass());
        RuntimeVerticle verticle = builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.VALUE_SERDE_CLASS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.VALUE_SERDE_CLASS_CONFIG), is(equalTo(Serdes.String().getClass().getName())));

    }

    @Test
    public void builderWithoutHttpServerOptions(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        RuntimeVerticle verticle = builder.build();

        assertFalse(verticle.serverOptions.isPresent());

    }

    @Test
    public void builderWithHttpServerOptions(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withHttpServer(new HttpServerOptions().setPort(1234));
        RuntimeVerticle verticle = builder.build();

        assertTrue(verticle.serverOptions.isPresent());
        assertThat(verticle.serverOptions.get().getPort(), is(equalTo(1234)));

    }

    @Test
    public void builderWithPort(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withHttpServer(4567);
        RuntimeVerticle verticle = builder.build();

        assertTrue(verticle.serverOptions.isPresent());
        assertThat(verticle.serverOptions.get().getPort(), is(equalTo(4567)));


    }
}
