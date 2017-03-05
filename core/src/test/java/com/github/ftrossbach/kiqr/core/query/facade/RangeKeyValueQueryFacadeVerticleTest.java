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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by ftr on 05/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class RangeKeyValueQueryFacadeVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(AllInstancesResponse.class, new KiqrCodec(AllInstancesResponse.class));        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverResponse.class, new KiqrCodec(InstanceResolverResponse.class));
        rule.vertx().eventBus().registerDefaultCodec(RangeKeyValueQuery.class, new KiqrCodec(RangeKeyValueQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(MultiValuedKeyValueQueryResponse.class, new KiqrCodec(MultiValuedKeyValueQueryResponse.class));

    }

    @Test
    public void successOneInstances(TestContext context){


        rule.vertx().eventBus().consumer(Config.ALL_INSTANCES, msg -> {
            Set<String> hosts = new HashSet<>();
            hosts.add("host1");
            msg.reply(new AllInstancesResponse(hosts));
        });

        rule.vertx().eventBus().consumer(Config.RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host1", msg -> {
            Map<String, String> result = new HashMap<>();
            result.put("key1", "value1");
            msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.OK, result));
        });


        rule.vertx().deployVerticle(new RangeKeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            RangeKeyValueQuery query = new RangeKeyValueQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName(), "key".getBytes(), "key".getBytes());

            rule.vertx().eventBus().send(Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof MultiValuedKeyValueQueryResponse);
                MultiValuedKeyValueQueryResponse response = (MultiValuedKeyValueQueryResponse) reply.body();
                context.assertEquals(QueryStatus.OK, response.getStatus());
                context.assertEquals(1, response.getResults().size());
                context.assertTrue(response.getResults().containsKey("key1"));
                context.assertEquals("value1", response.getResults().get("key1"));

            }));


        }));

    }

    @Test
    public void successTwoInstances(TestContext context){


        rule.vertx().eventBus().consumer(Config.ALL_INSTANCES, msg -> {
            Set<String> hosts = new HashSet<>();
            hosts.add("host1");
            hosts.add("host2");
            msg.reply(new AllInstancesResponse(hosts));
        });

        rule.vertx().eventBus().consumer(Config.RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host1", msg -> {
            Map<String, String> result = new HashMap<>();
            result.put("key1", "value1");
            msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.OK, result));
        });
        rule.vertx().eventBus().consumer(Config.RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host2", msg -> {
            Map<String, String> result = new HashMap<>();
            result.put("key2", "value2");
            msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.OK, result));
        });

        rule.vertx().deployVerticle(new RangeKeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            RangeKeyValueQuery query = new RangeKeyValueQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName(), "key".getBytes(), "key".getBytes());

            rule.vertx().eventBus().send(Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof MultiValuedKeyValueQueryResponse);
                MultiValuedKeyValueQueryResponse response = (MultiValuedKeyValueQueryResponse) reply.body();
                context.assertEquals(QueryStatus.OK, response.getStatus());
                context.assertEquals(2, response.getResults().size());
                context.assertTrue(response.getResults().containsKey("key1"));
                context.assertEquals("value1", response.getResults().get("key1"));
                context.assertTrue(response.getResults().containsKey("key2"));
                context.assertEquals("value2", response.getResults().get("key2"));

            }));


        }));

    }

    @Test
    public void failureOneSourceFails(TestContext context){


        rule.vertx().eventBus().consumer(Config.ALL_INSTANCES, msg -> {
            Set<String> hosts = new HashSet<>();
            hosts.add("host1");
            hosts.add("host2");
            msg.reply(new AllInstancesResponse(hosts));
        });

        rule.vertx().eventBus().consumer(Config.RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host1", msg -> {
            Map<String, String> result = new HashMap<>();
            result.put("key1", "value1");
            msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.OK, result));
        });
        rule.vertx().eventBus().consumer(Config.RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host2", msg -> {
           msg.fail(400, "msg");
        });

        rule.vertx().deployVerticle(new RangeKeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            RangeKeyValueQuery query = new RangeKeyValueQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName(), "key".getBytes(), "key".getBytes());

            rule.vertx().eventBus().send(Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));


        }));

    }

    @Test
    public void failureOfInstanceResolution(TestContext context){


        rule.vertx().eventBus().consumer(Config.ALL_INSTANCES, msg -> {
            msg.fail(400, "msg");
        });


        rule.vertx().deployVerticle(new RangeKeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            RangeKeyValueQuery query = new RangeKeyValueQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName(), "key".getBytes(), "key".getBytes());


            rule.vertx().eventBus().send(Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));


        }));

    }


}
