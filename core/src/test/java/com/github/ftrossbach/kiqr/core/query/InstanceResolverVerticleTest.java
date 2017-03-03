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
package com.github.ftrossbach.kiqr.core.query;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.AllInstancesResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverResponse;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;


import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ftr on 03/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class InstanceResolverVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Test(expected = IllegalArgumentException.class)
    public void constructorWithNullParam(){

        new InstanceResolverVerticle(null);

    }

    @Test
    public void constructorWithValidParam(){

        KafkaStreams streamMock = mock(KafkaStreams.class);
        try {
            new InstanceResolverVerticle(streamMock);
        } catch (Exception e) {
            fail();
        }

    }

    @Test
    public void deployment(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);
        InstanceResolverVerticle vut = new InstanceResolverVerticle(streamMock);
        rule.vertx().deployVerticle(vut, context.asyncAssertSuccess());

    }

    @Test
    public void allWithSingleSuccess(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);
        when(streamMock.allMetadataForStore("store")).thenReturn(Collections.singletonList(new StreamsMetadata(new HostInfo("host", 29), Collections.emptySet(), Collections.emptySet())));

        InstanceResolverVerticle vut = new InstanceResolverVerticle(streamMock);
        rule.vertx().deployVerticle(vut);
        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverQuery.class, new KiqrCodec(InstanceResolverQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(AllInstancesResponse.class, new KiqrCodec(AllInstancesResponse.class));
        rule.vertx().eventBus().send(Config.ALL_INSTANCES, "store", context.asyncAssertSuccess( handler -> {
            AllInstancesResponse response = (AllInstancesResponse) handler.body();

            context.assertEquals(1, response.getInstances().size());
            context.assertEquals("host", response.getInstances().iterator().next());


        }));


    }

    @Test
    public void allWithMultipleSuccess(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);
        List<StreamsMetadata> metadata = new ArrayList<>();
        metadata.add(new StreamsMetadata(new HostInfo("host1", 29),Collections.emptySet(), Collections.emptySet()));
        metadata.add(new StreamsMetadata(new HostInfo("host2", 29),Collections.emptySet(), Collections.emptySet()));

        when(streamMock.allMetadataForStore("store")).thenReturn(metadata);

        InstanceResolverVerticle vut = new InstanceResolverVerticle(streamMock);
        rule.vertx().deployVerticle(vut);
        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverQuery.class, new KiqrCodec(InstanceResolverQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(AllInstancesResponse.class, new KiqrCodec(AllInstancesResponse.class));
        rule.vertx().eventBus().send(Config.ALL_INSTANCES, "store", context.asyncAssertSuccess( handler -> {
            AllInstancesResponse response = (AllInstancesResponse) handler.body();

            context.assertEquals(2, response.getInstances().size());
            context.assertTrue(response.getInstances().contains("host1"));
            context.assertTrue(response.getInstances().contains("host2"));

        }));
    }

    @Test
    public void emptyReturnForMetadata(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);

        when(streamMock.allMetadataForStore("store")).thenReturn(Collections.emptyList());

        InstanceResolverVerticle vut = new InstanceResolverVerticle(streamMock);
        rule.vertx().deployVerticle(vut);
        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverQuery.class, new KiqrCodec(InstanceResolverQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(AllInstancesResponse.class, new KiqrCodec(AllInstancesResponse.class));
        rule.vertx().eventBus().send(Config.ALL_INSTANCES, "store", context.asyncAssertSuccess( handler -> {
            AllInstancesResponse response = (AllInstancesResponse) handler.body();


            context.assertTrue(response.getInstances().isEmpty());

        }));


    }

    @Test
    public void unexpectedRuntimeException(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);

        when(streamMock.allMetadataForStore("store")).thenThrow(IllegalArgumentException.class);

        InstanceResolverVerticle vut = new InstanceResolverVerticle(streamMock);
        rule.vertx().deployVerticle(vut);
        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverQuery.class, new KiqrCodec(InstanceResolverQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(AllInstancesResponse.class, new KiqrCodec(AllInstancesResponse.class));
        rule.vertx().eventBus().send(Config.ALL_INSTANCES, "store", context.asyncAssertFailure( handler -> {

           context.assertTrue(handler instanceof ReplyException);
           ReplyException ex = (ReplyException) handler;
           context.assertEquals(500, ex.failureCode());

        }));


    }



}
