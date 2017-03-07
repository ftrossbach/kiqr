/**
 * Copyright © 2017 Florian Troßbach (trossbach@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
    public void failingStart(TestContext context){

        KafkaStreams streamsMock = mock(KafkaStreams.class);
        KStreamBuilder builderMock = mock(KStreamBuilder.class);
        Properties props = new Properties();
        RuntimeVerticle verticleSpy = spy(new RuntimeVerticle(builderMock, props));

        doThrow(RuntimeException.class).when(verticleSpy).createAndStartStream();

        rule.vertx().deployVerticle(verticleSpy, context.asyncAssertFailure());

    }


    @Test
    public void builderApplicationId(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withApplicationId("test");
        RuntimeVerticle verticle = (RuntimeVerticle) builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.APPLICATION_ID_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.APPLICATION_ID_CONFIG), is(equalTo("test")));

    }

    @Test
    public void builderBootstrapServer(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withBootstrapServers("localhost:123");
        RuntimeVerticle verticle = (RuntimeVerticle) builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG), is(equalTo("localhost:123")));

    }

    @Test
    public void builderWithCaching(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withBuffering(42);
        RuntimeVerticle verticle = (RuntimeVerticle) builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG), is(equalTo(42)));

    }

    @Test
    public void builderWithoutCaching(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withoutBuffering();
        RuntimeVerticle verticle = (RuntimeVerticle) builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG), is(equalTo(0)));

    }

    @Test
    public void builderWithKeySerdeString(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withKeySerde(Serdes.String());
        RuntimeVerticle verticle = (RuntimeVerticle) builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.KEY_SERDE_CLASS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.KEY_SERDE_CLASS_CONFIG), is(equalTo(Serdes.String().getClass().getName())));

    }

    @Test
    public void builderWithKeySerdeClass(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withKeySerde(Serdes.String().getClass());
        RuntimeVerticle verticle = (RuntimeVerticle) builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.KEY_SERDE_CLASS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.KEY_SERDE_CLASS_CONFIG), is(equalTo(Serdes.String().getClass().getName())));

    }

    @Test
    public void builderWithValueSerdeString(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withValueSerde(Serdes.String());
        RuntimeVerticle verticle = (RuntimeVerticle) builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.VALUE_SERDE_CLASS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.VALUE_SERDE_CLASS_CONFIG), is(equalTo(Serdes.String().getClass().getName())));

    }

    @Test
    public void builderWithValueSerdeClass(){

        RuntimeVerticle.Builder builder = new RuntimeVerticle.Builder(new KStreamBuilder(), new Properties());

        builder.withValueSerde(Serdes.String().getClass());
        RuntimeVerticle verticle = (RuntimeVerticle) builder.build();

        assertThat(verticle.props, hasKey(StreamsConfig.VALUE_SERDE_CLASS_CONFIG));
        assertThat(verticle.props.get(StreamsConfig.VALUE_SERDE_CLASS_CONFIG), is(equalTo(Serdes.String().getClass().getName())));

    }


}
