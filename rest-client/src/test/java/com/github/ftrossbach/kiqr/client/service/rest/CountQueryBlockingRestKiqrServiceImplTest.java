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
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.KeyBasedQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.KeyValueStoreCountQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.StoreWideQuery;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
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
import java.util.Base64;
import java.util.Optional;

/**
 * Created by ftr on 07/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class CountQueryBlockingRestKiqrServiceImplTest {

    GenericBlockingRestKiqrClientImpl unitUnderTest = new GenericBlockingRestKiqrClientImpl("localhost", 4567);

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();


    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(KeyValueStoreCountQuery.class, new KiqrCodec(KeyValueStoreCountQuery.class));


    }


    @Test
    public void successfulQuery(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.COUNT_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

                msg.reply(42L);

            });

            rule.vertx().<Void>executeBlocking(future -> {

                Optional<Long> scalarKeyValue = unitUnderTest.count("store");
                context.assertTrue(scalarKeyValue.isPresent());
                context.assertEquals(42L, scalarKeyValue.get());
                async.complete();

                future.complete();
            }, context.asyncAssertSuccess());

        }));

    }

    @Test
    public void notFound(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.COUNT_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(404, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {

                Optional<Long> scalarKeyValue = unitUnderTest.count("idontexist");
                context.assertFalse(scalarKeyValue.isPresent());
                async.complete();

                future.complete();
            }, context.asyncAssertSuccess());

        }));

    }



    @Test
    public void internalServerError(TestContext context){
        Async async = context.async();

        rule.vertx().deployVerticle(new MockedRuntimeHttpServerVerticle(new HttpServerOptions().setPort(4567), new DummyVerticle()), context.asyncAssertSuccess(handler -> {

            rule.vertx().eventBus().consumer(Config.COUNT_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

                msg.fail(500, "does not matter");

            });

            rule.vertx().<Void>executeBlocking(future -> {


                Optional<Long> scalarKeyValue = unitUnderTest.count("store");
                context.fail();



            }, context.asyncAssertFailure(ex -> {
                context.assertTrue(ex instanceof QueryExecutionException);
                async.complete();
            }));

        }));

    }


}
