package com.github.ftrossbach.kiqr.core;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.Properties;


import static org.mockito.Mockito.*;

/**
 * Created by ftr on 06/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class RuntimeVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Test
    public void successfulStartIfAllPropertiesAreSet(TestContext context){

        KafkaStreams streamsMock = mock(KafkaStreams.class);
        KStreamBuilder builderMock = mock(KStreamBuilder.class);
        Properties props = new Properties();
        RuntimeVerticle verticleSpy = spy(new RuntimeVerticle(builderMock, props));

        doReturn(streamsMock).when(verticleSpy).createAndStartStream();

        rule.vertx().deployVerticle(verticleSpy, context.asyncAssertSuccess(handler -> {


            context.assertTrue(rule.vertx().deploymentIDs().size() > 0);
        }));

    }
}
