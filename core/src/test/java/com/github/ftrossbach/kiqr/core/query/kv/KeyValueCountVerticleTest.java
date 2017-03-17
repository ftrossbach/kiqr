package com.github.ftrossbach.kiqr.core.query.kv;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.KeyValueStoreCountQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.MultiValuedKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.RangeKeyValueQuery;
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


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ftr on 17/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class KeyValueCountVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(KeyValueStoreCountQuery.class, new KiqrCodec(KeyValueStoreCountQuery.class));
    }

    @Test
    public void success(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);
        KeyValueIterator<Object, Object> iteratorMock = mock(KeyValueIterator.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.approximateNumEntries()).thenReturn(42L);


        rule.vertx().deployVerticle(new KeyValueCountVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyValueStoreCountQuery query = new KeyValueStoreCountQuery("store");

            rule.vertx().eventBus().send(Config.COUNT_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof Long);
                context.assertEquals(42L, reply.body());

            }));


        }));


    }





    @Test
    public void illegalStateStoreExceptionOnQuery(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);
        KeyValueIterator<Object, Object> iteratorMock = mock(KeyValueIterator.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.approximateNumEntries()).thenThrow(InvalidStateStoreException.class);

        rule.vertx().deployVerticle(new KeyValueCountVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyValueStoreCountQuery query = new KeyValueStoreCountQuery("store");

            rule.vertx().eventBus().send(Config.COUNT_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));

        }));

    }

    @Test
    public void unexpectedExceptionOnQuery(TestContext context){
        KafkaStreams streamMock = mock(KafkaStreams.class);
        ReadOnlyKeyValueStore<Object, Object> storeMock = mock(ReadOnlyKeyValueStore.class);
        KeyValueIterator<Object, Object> iteratorMock = mock(KeyValueIterator.class);

        when(streamMock.store(eq("store"), any(QueryableStoreType.class))).thenReturn(storeMock);
        when(storeMock.approximateNumEntries()).thenThrow(IllegalArgumentException.class);

        rule.vertx().deployVerticle(new KeyValueCountVerticle("host", streamMock), context.asyncAssertSuccess(deployment->{

            KeyValueStoreCountQuery query = new KeyValueStoreCountQuery("store");

            rule.vertx().eventBus().send(Config.COUNT_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());

            }));

        }));

    }
}
