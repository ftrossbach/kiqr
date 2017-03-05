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
import com.github.ftrossbach.kiqr.core.query.SimpleKeyValueIterator;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import sun.java2d.pipe.SpanShapeRenderer;


import java.util.Collections;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ftr on 05/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class AllKeyValuesQueryVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(AllInstancesResponse.class, new KiqrCodec(AllInstancesResponse.class));
        rule.vertx().eventBus().registerDefaultCodec(AllKeyValuesQuery.class, new KiqrCodec(AllKeyValuesQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(MultiValuedKeyValueQueryResponse.class, new KiqrCodec(MultiValuedKeyValueQueryResponse.class));
    }

    @Test
    public void successWithSingleResult(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);
        KeyValueIterator<Object, Object> iteratorMock = mock(KeyValueIterator.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        SimpleKeyValueIterator iterator = new SimpleKeyValueIterator(new KeyValue<Object, Object>("key", "value"));

        when(storeMock.all()).thenReturn(iterator);


        rule.vertx().deployVerticle(new AllKeyValuesQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof MultiValuedKeyValueQueryResponse);
                MultiValuedKeyValueQueryResponse response = (MultiValuedKeyValueQueryResponse) reply.body();
                context.assertEquals(QueryStatus.OK, response.getStatus());
                context.assertEquals(1, response.getResults().size());
                context.assertTrue(response.getResults().containsKey("a2V5"));//"key" as UT8/b64
                context.assertEquals("dmFsdWU=", response.getResults().get("a2V5"));//"value" as UT8/b64

                context.assertTrue(iterator.closed);

            }));


        }));


    }

    @Test
    public void successWithDoubleResult(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);

        SimpleKeyValueIterator iterator = new SimpleKeyValueIterator(new KeyValue<Object, Object>("key", "value"), new KeyValue<Object, Object>("key2", "value2"));
        when(storeMock.all()).thenReturn(iterator);


        rule.vertx().deployVerticle(new AllKeyValuesQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof MultiValuedKeyValueQueryResponse);
                MultiValuedKeyValueQueryResponse response = (MultiValuedKeyValueQueryResponse) reply.body();
                context.assertEquals(QueryStatus.OK, response.getStatus());
                context.assertEquals(2, response.getResults().size());
                context.assertTrue(response.getResults().containsKey("a2V5"));//"key" as UT8/b64
                context.assertEquals("dmFsdWU=", response.getResults().get("a2V5"));//"value" as UT8/b64

                context.assertTrue(response.getResults().containsKey("a2V5Mg=="));//"key2" as UT8/b64
                context.assertEquals("dmFsdWUy", response.getResults().get("a2V5Mg=="));//"value" as UT8/b64

                context.assertTrue(iterator.closed);

            }));

        }));

    }

    @Test
    public void notFoundWithNoResult(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);
        KeyValueIterator<Object, Object> iteratorMock = mock(KeyValueIterator.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        SimpleKeyValueIterator iterator = new SimpleKeyValueIterator();
        when(storeMock.all()).thenReturn(iterator);


        rule.vertx().deployVerticle(new AllKeyValuesQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof MultiValuedKeyValueQueryResponse);
                MultiValuedKeyValueQueryResponse response = (MultiValuedKeyValueQueryResponse) reply.body();
                context.assertEquals(QueryStatus.NOT_FOUND, response.getStatus());
                context.assertEquals(0, response.getResults().size());
                context.assertTrue(iterator.closed);

            }));

        }));

    }

    @Test
    public void noValidKeySerde(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);

        rule.vertx().deployVerticle(new AllKeyValuesQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", "i am not a serde", Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));

        }));

    }

    @Test
    public void noValidValueSerde(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);

        rule.vertx().deployVerticle(new AllKeyValuesQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), "i am not a serde" );

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));

        }));

    }

    @Test
    public void illegalStateStoreExceptionOnStoreInitialization(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class) )).thenThrow( InvalidStateStoreException.class);

        rule.vertx().deployVerticle(new AllKeyValuesQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(409, ex.failureCode());

            }));

        }));

    }

    @Test
    public void illegalStateStoreExceptionOnQuery(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.all()).thenThrow(InvalidStateStoreException.class);

        rule.vertx().deployVerticle(new AllKeyValuesQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(409, ex.failureCode());

            }));

        }));

    }

    @Test
    public void unexpectedExceptionOnQuery(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.all()).thenThrow(IllegalArgumentException.class);

        rule.vertx().deployVerticle(new AllKeyValuesQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));

        }));

    }
}
