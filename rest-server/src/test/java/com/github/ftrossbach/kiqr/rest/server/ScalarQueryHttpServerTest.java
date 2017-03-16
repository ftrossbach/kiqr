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
package com.github.ftrossbach.kiqr.rest.server;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.KeyBasedQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.core.RuntimeVerticle;
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
import static org.mockito.Mockito.*;

/**
 * Created by ftr on 06/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class ScalarQueryHttpServerTest {

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();



    @Before
    public void setUp(TestContext context) throws Exception{
        rule.vertx().eventBus().registerDefaultCodec(KeyBasedQuery.class, new KiqrCodec(KeyBasedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(ScalarKeyValueQueryResponse.class, new KiqrCodec(ScalarKeyValueQueryResponse.class));


        rule.vertx().deployVerticle(new RestKiqrServerVerticle(new HttpServerOptions().setPort(5762), new DummySuccessfulVerticle()), context.asyncAssertSuccess());
    }


    @Test
    public void readScalarValue(TestContext context) throws Exception {

        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);



        rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {
            context.assertTrue(msg.body() instanceof KeyBasedQuery);
            KeyBasedQuery query = (KeyBasedQuery) msg.body();

            context.assertEquals("store", query.getStoreName());
            context.assertEquals(Serdes.String().getClass().getName(), query.getKeySerde());
            context.assertEquals(Serdes.Long().getClass().getName(), query.getValueSerde());
            context.assertTrue(query.getKey().length > 0);

            msg.reply(new ScalarKeyValueQueryResponse("test"));

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store/values/key?keySerde=%s&valueSerde=%s", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(200, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void scalarValueNotFound(TestContext context) {

        Async async = context.async();


        rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

            msg.fail(404, "irrelevant");

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store/values/key?keySerde=%s&valueSerde=%s", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(404, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void scalarValueNoKeySerde(TestContext context) {

        Async async = context.async();


        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store/values/key?valueSerde=%s", Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(400, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void scalarValueNoValueSerde(TestContext context) {

        Async async = context.async();


        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store/values/key?keySerde=%s", Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(400, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void scalarInternalServerError(TestContext context){
        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);



        rule.vertx().eventBus().consumer(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {
            msg.fail(500, "msg");

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store/values/key?keySerde=%s&valueSerde=%s", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(500, res.statusCode());
            async.complete();
        }).end();
    }




}
