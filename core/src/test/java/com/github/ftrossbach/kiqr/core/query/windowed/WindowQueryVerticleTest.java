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
package com.github.ftrossbach.kiqr.core.query.windowed;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.QueryStatus;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQueryResponse;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import com.github.ftrossbach.kiqr.core.query.SimpleWindowStoreIterator;
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
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ftr on 05/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class WindowQueryVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(WindowedQuery.class, new KiqrCodec(WindowedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(WindowedQueryResponse.class, new KiqrCodec(WindowedQueryResponse.class));
    }

    @Test
    public void successWithSingleResult(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyWindowStore<Object, Object> storeMock = mock(ReadOnlyWindowStore.class);
        KeyValueIterator<Object, Object> iteratorMock = mock(KeyValueIterator.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        SimpleWindowStoreIterator iterator = new SimpleWindowStoreIterator(new KeyValue<Long, Object>(1L, "value"));



        when(storeMock.fetch(any(), anyLong(), anyLong())).thenReturn(iterator);

        rule.vertx().deployVerticle(new WindowedQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName(), 0, 1);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof WindowedQueryResponse);
                WindowedQueryResponse response = (WindowedQueryResponse) reply.body();
                context.assertEquals(QueryStatus.OK, response.getStatus());
                context.assertEquals(1, response.getValues().size());
                context.assertTrue(response.getValues().containsKey(1L));
                context.assertEquals("dmFsdWU=", response.getValues().get(1L));//"value" as UT8/b64
                context.assertTrue(iterator.closed);

            }));


        }));


    }

    @Test
    public void successWithDoubleResult(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyWindowStore<Object, Object> storeMock = mock(ReadOnlyWindowStore.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);

        SimpleWindowStoreIterator iterator = new SimpleWindowStoreIterator(new KeyValue<Long, Object>(1L, "value"), new KeyValue<Long, Object>(2L, "value2"));

        when(storeMock.fetch(any(), anyLong(), anyLong())).thenReturn(iterator);


        rule.vertx().deployVerticle(new WindowedQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName(), 0, 1);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof WindowedQueryResponse);
                WindowedQueryResponse response = (WindowedQueryResponse) reply.body();
                context.assertEquals(QueryStatus.OK, response.getStatus());
                context.assertEquals(2, response.getValues().size());
                context.assertTrue(response.getValues().containsKey(1L));
                context.assertEquals("dmFsdWU=", response.getValues().get(1L));//"value" as UT8/b64

                context.assertTrue(response.getValues().containsKey(2L));
                context.assertEquals("dmFsdWUy", response.getValues().get(2L));//"value" as UT8/b64

                context.assertTrue(iterator.closed);

            }));

        }));

    }

    @Test
    public void notFoundWithNoResult(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyWindowStore<Object, Object> storeMock = mock(ReadOnlyWindowStore.class);
        KeyValueIterator<Object, Object> iteratorMock = mock(KeyValueIterator.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        SimpleWindowStoreIterator iterator = new SimpleWindowStoreIterator();

        when(storeMock.fetch(any(), anyLong(), anyLong())).thenReturn(iterator);


        rule.vertx().deployVerticle(new WindowedQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName(), 0, 1);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof WindowedQueryResponse);
                WindowedQueryResponse response = (WindowedQueryResponse) reply.body();
                context.assertEquals(QueryStatus.NOT_FOUND, response.getStatus());
                context.assertEquals(0, response.getValues().size());
                context.assertTrue(iterator.closed);

            }));

        }));

    }

    @Test
    public void noValidKeySerde(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);

        rule.vertx().deployVerticle(new WindowedQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", "i am not a serde", "key".getBytes(),  Serdes.String().getClass().getName(), 0, 1);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));

        }));

    }

    @Test
    public void noValidValueSerde(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);

        rule.vertx().deployVerticle(new WindowedQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  "i am not a serde", 0, 1);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

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

        rule.vertx().deployVerticle(new WindowedQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName(), 0, 1);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(409, ex.failureCode());

            }));

        }));

    }

    @Test
    public void illegalStateStoreExceptionOnQuery(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyWindowStore<Object, Object> storeMock = mock(ReadOnlyWindowStore.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.fetch(any(), anyLong(), anyLong())).thenThrow(InvalidStateStoreException.class);

        rule.vertx().deployVerticle(new WindowedQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName(), 0, 1);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(409, ex.failureCode());

            }));

        }));

    }

    @Test
    public void unexpectedExceptionOnQuery(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyWindowStore<Object, Object> storeMock = mock(ReadOnlyWindowStore.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.fetch(any(), anyLong(), anyLong())).thenThrow(IllegalArgumentException.class);


        rule.vertx().deployVerticle(new WindowedQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName(), 0, 1);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));

        }));

    }
    
}
