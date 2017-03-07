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
import java.util.Optional;


import static org.mockito.Mockito.mock;

/**
 * Created by ftr on 05/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class KeyValueQueryFacadeVerticleTest {
    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverQuery.class, new KiqrCodec(InstanceResolverQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(InstanceResolverResponse.class, new KiqrCodec(InstanceResolverResponse.class));
        rule.vertx().eventBus().registerDefaultCodec(ScalarKeyValueQuery.class, new KiqrCodec(ScalarKeyValueQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(ScalarKeyValueQueryResponse.class, new KiqrCodec(ScalarKeyValueQueryResponse.class));

    }

    @Test
    public void success(TestContext context){

        rule.vertx().eventBus().consumer(Config.INSTANCE_RESOLVER_ADDRESS_SINGLE, msg -> {
            msg.reply(new InstanceResolverResponse(Optional.of("host")));
        });

        rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", msg -> {
           msg.reply(new ScalarKeyValueQueryResponse("value"));
        });

        rule.vertx().deployVerticle(new KeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            ScalarKeyValueQuery query = new ScalarKeyValueQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof ScalarKeyValueQueryResponse);
                ScalarKeyValueQueryResponse response = (ScalarKeyValueQueryResponse) reply.body();
                context.assertEquals("value", response.getValue());

            }));


        }));

    }

    @Test
    public void forwardingFailureDuringQuery(TestContext context){


        rule.vertx().eventBus().consumer(Config.INSTANCE_RESOLVER_ADDRESS_SINGLE, msg -> {
            msg.reply(new InstanceResolverResponse(Optional.of("host")));
        });

        rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + "host", msg -> {
            msg.fail(400, "msg");
        });

        rule.vertx().deployVerticle(new KeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            ScalarKeyValueQuery query = new ScalarKeyValueQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

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



        rule.vertx().deployVerticle(new KeyValueQueryFacadeVerticle(), context.asyncAssertSuccess(deployment->{

            ScalarKeyValueQuery query = new ScalarKeyValueQuery("store", Serdes.String().getClass().getName(), "key".getBytes(), Serdes.String().getClass().getName());

            rule.vertx().eventBus().send(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(500, ex.failureCode());
                context.assertEquals("msg", ex.getMessage());

            }));
        }));

    }

}
