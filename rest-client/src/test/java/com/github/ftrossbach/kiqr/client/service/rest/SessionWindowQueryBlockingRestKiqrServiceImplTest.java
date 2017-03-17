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
package com.github.ftrossbach.kiqr.client.service.rest;

import com.github.ftrossbach.kiqr.client.service.ConnectionException;
import com.github.ftrossbach.kiqr.client.service.QueryExecutionException;
import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.Async;
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
 * Created by ftr on 07/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class SessionWindowQueryBlockingRestKiqrServiceImplTest {

    GenericBlockingRestKiqrClientImpl unitUnderTest = new GenericBlockingRestKiqrClientImpl("localhost", 4567);

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    String keyAsByteArray1 = Base64.getEncoder().encodeToString(Serdes.String().serializer().serialize("", "key1"));
    String keyAsByteArray2 = Base64.getEncoder().encodeToString(Serdes.String().serializer().serialize("", "key2"));


    String valueAsByteArray1 = Base64.getEncoder().encodeToString(Serdes.Long().serializer().serialize("", 42L));
    String valueAsByteArray2 = Base64.getEncoder().encodeToString(Serdes.Long().serializer().serialize("", 4711L));

    @Before
    public void setUp() {
        rule.vertx().eventBus().registerDefaultCodec(KeyBasedQuery.class, new KiqrCodec(KeyBasedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(SessionQueryResponse.class, new KiqrCodec(SessionQueryResponse.class));


    }


    @Test
    public void successfulQuery(TestContext context) {
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.SESSION_QUERY_FACADE_ADDRESS, msg -> {





                List<Window> results = new ArrayList<>();
                results.add(new Window(0,100, valueAsByteArray1));
                results.add(new Window(101,200, valueAsByteArray2));

                msg.reply(new SessionQueryResponse(results));

            });

            rule.vertx().<Void>executeBlocking((Future<Void> future) -> {

                Map<Window, Long> result = unitUnderTest.getSession("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.assertFalse(result.isEmpty());
                context.assertEquals(2, result.size());

                context.assertTrue(result.containsValue(4711L));
                context.assertTrue(result.containsValue(42L));
                async.complete();

                future.complete();
            }, context.asyncAssertSuccess());

        }));

    }

    @Test
    public void notFound(TestContext context) {
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.SESSION_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(404, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {


                Map<Window, Long> result = unitUnderTest.getSession("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.assertTrue(result.isEmpty());
                async.complete();

                future.complete();
            }, context.asyncAssertSuccess());

        }));

    }

    @Test
    public void badRequest(TestContext context) {
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.SESSION_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(400, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {

                Map<Window, Long> result = unitUnderTest.getSession("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.fail();


            }, context.asyncAssertFailure(ex -> {

                context.assertTrue(ex instanceof IllegalArgumentException);
                async.complete();
            }));

        }));

    }

    @Test
    public void internalServerError(TestContext context) {
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.SESSION_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(500, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {

                Map<Window, Long> result = unitUnderTest.getSession("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long());
                context.fail();


            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof QueryExecutionException);
                async.complete();
            }));

        }));

    }



}
