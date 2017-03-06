package com.github.ftrossbach.kiqr.core.query.facade;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by ftr on 06/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class WindowQueryFacadeVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverQuery.class, new KiqrCodec(InstanceResolverQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverResponse.class, new KiqrCodec(InstanceResolverResponse.class));

        rule.vertx().eventBus().registerDefaultCodec(WindowedQuery.class, new KiqrCodec(WindowedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(WindowedQueryResponse.class, new KiqrCodec(WindowedQueryResponse.class));

    }

    @Test
    public void success(TestContext context){

        rule.vertx().eventBus().consumer(Config.INSTANCE_RESOLVER_ADDRESS_SINGLE, msg -> {
            msg.reply(new InstanceResolverResponse(QueryStatus.OK, Optional.of("host")));
        });

        rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", msg -> {
            SortedMap<Long, String> result = new TreeMap<>();

            result.put(1L, "abc");
            result.put(2L, "def");
            msg.reply(new WindowedQueryResponse(QueryStatus.OK, result ));
        });

        rule.vertx().deployVerticle(new WindowedQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),Serdes.String().getClass().getName(),  1, 2);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof WindowedQueryResponse);
                WindowedQueryResponse response = (WindowedQueryResponse) reply.body();
                context.assertEquals(QueryStatus.OK, response.getStatus());
                context.assertEquals(2, response.getValues().size());
                context.assertTrue(response.getValues().containsKey(1L));
                context.assertEquals("abc", response.getValues().get(1L));
                context.assertTrue(response.getValues().containsKey(2L));
                context.assertEquals("def", response.getValues().get(2L));

            }));


        }));

    }

    @Test
    public void forwardingFailureDuringQuery(TestContext context){


        rule.vertx().eventBus().consumer(Config.INSTANCE_RESOLVER_ADDRESS_SINGLE, msg -> {
            msg.reply(new InstanceResolverResponse(QueryStatus.OK, Optional.of("host")));
        });

        rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", msg -> {
            msg.fail(400, "msg");
        });

        rule.vertx().deployVerticle(new WindowedQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),Serdes.String().getClass().getName(),  1, 2);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());
                context.assertEquals("msg", ex.getMessage());

            }));
        }));

    }

    @Test
    public void forwardingFailureDuringInstanceLookup(TestContext context){

        rule.vertx().eventBus().consumer(Config.INSTANCE_RESOLVER_ADDRESS_SINGLE, msg -> {
            msg.fail(500, "msg");
        });



        rule.vertx().deployVerticle(new WindowedQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),Serdes.String().getClass().getName(),  1, 2);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());
                context.assertEquals("msg", ex.getMessage());

            }));
        }));

    }
}
