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
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.MultiValuedKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.RangeKeyValueQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQueryResponse;
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
public class WindowQueryBlockingRestKiqrServiceImplTest {

    BlockingRestKiqrServiceImpl unitUnderTest = new BlockingRestKiqrServiceImpl("localhost", 4567);

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    String keyAsByteArray1 = Base64.getEncoder().encodeToString(Serdes.String().serializer().serialize("", "key1"));
    String keyAsByteArray2 = Base64.getEncoder().encodeToString(Serdes.String().serializer().serialize("", "key2"));


    String valueAsByteArray1 = Base64.getEncoder().encodeToString(Serdes.Long().serializer().serialize("", 42L));
    String valueAsByteArray2 = Base64.getEncoder().encodeToString(Serdes.Long().serializer().serialize("", 4711L));

    @Before
    public void setUp() {
        rule.vertx().eventBus().registerDefaultCodec(WindowedQuery.class, new KiqrCodec(WindowedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(WindowedQueryResponse.class, new KiqrCodec(WindowedQueryResponse.class));


    }


    @Test
    public void successfulQuery(TestContext context) {
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_FACADE_ADDRESS, msg -> {


                SortedMap<Long, String> result = new TreeMap<>();
                result.put(1L, valueAsByteArray1);
                result.put(2L, valueAsByteArray2);


                msg.reply(new WindowedQueryResponse(result));

            });

            rule.vertx().<Void>executeBlocking((Future<Void> future) -> {

                Map<Long, Long> result = unitUnderTest.getWindow("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long(), 1L, 2L);
                context.assertFalse(result.isEmpty());
                context.assertEquals(2, result.size());
                context.assertNotNull(result.get(1L));
                context.assertEquals(42L, result.get(1L));
                context.assertNotNull(result.get(2L));
                context.assertEquals(4711L, result.get(2L));
                async.complete();

                future.complete();
            }, context.asyncAssertSuccess());

        }));

    }

    @Test
    public void notFound(TestContext context) {
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(404, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {


                Map<Long, Long> result = unitUnderTest.getWindow("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long(), 1L, 2L);
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

            rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(400, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {

                Map<Long, Long> result = unitUnderTest.getWindow("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long(), 1L, 2L);
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

            rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(500, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {

                Map<Long, Long> result = unitUnderTest.getWindow("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long(), 1L, 2L);
                context.fail();


            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof QueryExecutionException);
                async.complete();
            }));

        }));

    }

    @Test
    public void connectionExceptionInvalidPort(TestContext context) {


        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().<Void>executeBlocking(future -> {

                unitUnderTest = new BlockingRestKiqrServiceImpl("localhost", -1);

                Map<Long, Long> result = unitUnderTest.getWindow("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long(), 1L, 2L);
                context.fail();


            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof ConnectionException);
                async.complete();
            }));

        }));

    }

    @Test
    public void connectionExceptionInvalidHost(TestContext context) {
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().<Void>executeBlocking(future -> {

                unitUnderTest = new BlockingRestKiqrServiceImpl("host with spaces", 4567);

                Map<Long, Long> result = unitUnderTest.getWindow("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long(), 1L, 2L);
                context.fail();


            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof ConnectionException);
                async.complete();
            }));

        }));

    }

    @Test
    public void validButUnReachableHost(TestContext context) {
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().<Void>executeBlocking(future -> {

                unitUnderTest = new BlockingRestKiqrServiceImpl("ihopethisdoesntexist", 4567);
                Map<Long, Long> result = unitUnderTest.getWindow("store", String.class, "key", Long.class, Serdes.String(), Serdes.Long(), 1L, 2L);
                context.fail();


            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof ConnectionException);
                async.complete();
            }));

        }));

    }

}
