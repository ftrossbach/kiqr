package com.github.ftrossbach.kiqr.core.query.session;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.KeyBasedQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.SessionQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQueryResponse;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import com.github.ftrossbach.kiqr.core.query.SimpleKeyValueIterator;
import com.github.ftrossbach.kiqr.core.query.SimpleSessionIterator;
import com.github.ftrossbach.kiqr.core.query.SimpleWindowStoreIterator;
import com.github.ftrossbach.kiqr.core.query.windowed.WindowedQueryVerticle;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
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
 * Created by ftr on 17/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class SessionWindowQueryVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(KeyBasedQuery.class, new KiqrCodec(KeyBasedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(SessionQueryResponse.class, new KiqrCodec(SessionQueryResponse.class));
    }

    @Test
    public void successWithSingleResult(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlySessionStore<Object, Object> storeMock = mock(ReadOnlySessionStore.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        SimpleSessionIterator iterator = new SimpleSessionIterator(new KeyValue<Windowed<Object>, Object>(new Windowed<>("key", new TimeWindow(0L, 1L)), "value"));


        when(storeMock.fetch("key")).thenReturn(iterator);

        rule.vertx().deployVerticle(new SessionWindowQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.SESSION_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof SessionQueryResponse);
                SessionQueryResponse response = (SessionQueryResponse) reply.body();
                context.assertEquals(1, response.getValues().size());

                context.assertEquals("dmFsdWU=", response.getValues().get(0).getValue());//"value" as UT8/b64
                context.assertTrue(iterator.closed);

            }));


        }));


    }



    @Test
    public void notFoundWithNoResult(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlySessionStore<Object, Object> storeMock = mock(ReadOnlySessionStore.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        SimpleSessionIterator iterator = new SimpleSessionIterator();


        when(storeMock.fetch(any())).thenReturn(iterator);

        rule.vertx().deployVerticle(new SessionWindowQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.SESSION_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof SessionQueryResponse);
                SessionQueryResponse response = (SessionQueryResponse) reply.body();
                context.assertEquals(0, response.getValues().size());
                context.assertTrue(iterator.closed);

            }));

        }));

    }

    @Test
    public void noValidKeySerde(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);

        rule.vertx().deployVerticle(new SessionWindowQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", "i am not a serde", "key".getBytes(),  Serdes.String().getClass().getName());
             rule.vertx().eventBus().send(Config.SESSION_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));

        }));

    }

    @Test
    public void noValidValueSerde(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);

        rule.vertx().deployVerticle(new SessionWindowQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  "i am not a serde");

            rule.vertx().eventBus().send(Config.SESSION_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

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

        rule.vertx().deployVerticle(new SessionWindowQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.SESSION_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));

        }));

    }

    @Test
    public void illegalStateStoreExceptionOnQuery(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlySessionStore<Object, Object> storeMock = mock(ReadOnlySessionStore.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.fetch(any())).thenThrow(InvalidStateStoreException.class);

        rule.vertx().deployVerticle(new SessionWindowQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.SESSION_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));

        }));

    }

    @Test
    public void unexpectedExceptionOnQuery(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlySessionStore<Object, Object> storeMock = mock(ReadOnlySessionStore.class);
        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.fetch(any())).thenThrow(IllegalArgumentException.class);


        rule.vertx().deployVerticle(new SessionWindowQueryVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyBasedQuery query = new KeyBasedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),  Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.SESSION_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));

        }));

    }
}
