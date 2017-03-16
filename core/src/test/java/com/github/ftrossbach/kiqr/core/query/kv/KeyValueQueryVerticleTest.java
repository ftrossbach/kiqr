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
package com.github.ftrossbach.kiqr.core.query.kv;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ftr on 05/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class KeyValueQueryVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(KeyBasedQuery.class, new KiqrCodec(KeyBasedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(ScalarKeyValueQueryResponse.class, new KiqrCodec(ScalarKeyValueQueryResponse.class));
    }

    @Test
    public void success(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);


        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);


        when(storeMock.get("key")).thenReturn("value");


        rule.vertx().deployVerticle(new KeyValueQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof ScalarKeyValueQueryResponse);
                ScalarKeyValueQueryResponse response = (ScalarKeyValueQueryResponse) reply.body();
                context.assertEquals("dmFsdWU=", response.getValue());

            }));


        }));

    }

    @Test
    public void notFound(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);


        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);


        when(storeMock.get("key")).thenReturn(null);


        rule.vertx().deployVerticle(new KeyValueQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);

                ReplyException exception = (ReplyException) handler;
                context.assertEquals(404, exception.failureCode());

            }));


        }));


    }

    @Test
    public void illegalStateStoreExceptionOnStoreInitialization(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenThrow(InvalidStateStoreException.class);


        rule.vertx().deployVerticle(new KeyValueQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));


        }));

    }

    @Test
    public void illegalStateStoreExceptionOnQuery(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);

        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.get(any())).thenThrow(InvalidStateStoreException.class);
        rule.vertx().deployVerticle(new KeyValueQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));


        }));

    }

    @Test
    public void unexpectedRuntimeException(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);

        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.get(any())).thenThrow(IllegalArgumentException.class);
        rule.vertx().deployVerticle(new KeyValueQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));


        }));

    }

    @Test
    public void invalidKeySerde(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);

        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.get(any())).thenThrow(IllegalArgumentException.class);
        rule.vertx().deployVerticle(new KeyValueQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", "i am not a serde", "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));


        }));

    }

    @Test
    public void invalidValueSerde(TestContext context){

        KafkaStreams streamMock = mock(KafkaStreams.class);

        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.get(any())).thenThrow(IllegalArgumentException.class);
        rule.vertx().deployVerticle(new KeyValueQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), "i am not a serde");

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));


        }));

    }

}
