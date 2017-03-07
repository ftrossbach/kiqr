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
import java.util.*;

/**
 * Created by ftr on 05/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class AllKeyValueQueryFacadeVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(AllInstancesResponse.class, new KiqrCodec(AllInstancesResponse.class));
        rule.vertx().eventBus().registerDefaultCodec(AllKeyValuesQuery.class, new KiqrCodec(AllKeyValuesQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(MultiValuedKeyValueQueryResponse.class, new KiqrCodec(MultiValuedKeyValueQueryResponse.class));

    }

    @Test
    public void successOneInstances(TestContext context){


        rule.vertx().eventBus().consumer(Config.ALL_INSTANCES, msg -> {
            Set<String> hosts = new HashSet<>();
            hosts.add("host1");
            msg.reply(new AllInstancesResponse(hosts));
        });

        rule.vertx().eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host1", msg -> {
            Map<String, String> result = new HashMap<>();
            result.put("key1", "value1");
            msg.reply(new MultiValuedKeyValueQueryResponse(result));
        });


        rule.vertx().deployVerticle(new AllKeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof MultiValuedKeyValueQueryResponse);
                MultiValuedKeyValueQueryResponse response = (MultiValuedKeyValueQueryResponse) reply.body();
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

        rule.vertx().eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host1", msg -> {
            Map<String, String> result = new HashMap<>();
            result.put("key1", "value1");
            msg.reply(new MultiValuedKeyValueQueryResponse(result));
        });
        rule.vertx().eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host2", msg -> {
            Map<String, String> result = new HashMap<>();
            result.put("key2", "value2");
            msg.reply(new MultiValuedKeyValueQueryResponse(result));
        });

        rule.vertx().deployVerticle(new AllKeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof MultiValuedKeyValueQueryResponse);
                MultiValuedKeyValueQueryResponse response = (MultiValuedKeyValueQueryResponse) reply.body();
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

        rule.vertx().eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host1", msg -> {
            Map<String, String> result = new HashMap<>();
            result.put("key1", "value1");
            msg.reply(new MultiValuedKeyValueQueryResponse(result));
        });
        rule.vertx().eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + "host2", msg -> {
           msg.fail(400, "msg");
        });

        rule.vertx().deployVerticle(new AllKeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

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


        rule.vertx().deployVerticle(new AllKeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            AllKeyValuesQuery query = new AllKeyValuesQuery("store", Serdes.String().getClass().getName(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());

            }));


        }));

    }


}
