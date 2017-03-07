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
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
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


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;


import static org.mockito.Mockito.mock;

/**
 * Created by ftr on 06/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class MultiValuedQueryHttpServerTest {

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();



    @Before
    public void setUp(TestContext context) throws Exception{
        rule.vertx().eventBus().registerDefaultCodec(AllKeyValuesQuery.class, new KiqrCodec(AllKeyValuesQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(RangeKeyValueQuery.class, new KiqrCodec<>(RangeKeyValueQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(MultiValuedKeyValueQueryResponse.class, new KiqrCodec(MultiValuedKeyValueQueryResponse.class));


        rule.vertx().deployVerticle(new HttpServer(new HttpServerOptions().setPort(5762), new DummySuccessfulVerticle()), context.asyncAssertSuccess());
    }


    @Test
    public void readAllValue(TestContext context) throws Exception {

        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);



        rule.vertx().eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {
            context.assertTrue(msg.body() instanceof AllKeyValuesQuery);
            AllKeyValuesQuery query = (AllKeyValuesQuery) msg.body();

            context.assertEquals("store", query.getStoreName());
            context.assertEquals(Serdes.String().getClass().getName(), query.getKeySerde());
            context.assertEquals(Serdes.Long().getClass().getName(), query.getValueSerde());

            Map<String, String> results = new HashMap<>();
            results.put("key1", "value1");
            results.put("key2", "value2");

            msg.reply(new MultiValuedKeyValueQueryResponse(results));

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store?keySerde=%s&valueSerde=%s", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(200, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void valuesEmpty(TestContext context) {

        Async async = context.async();


        rule.vertx().eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {

            msg.reply(new MultiValuedKeyValueQueryResponse(Collections.emptyMap()));

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store?keySerde=%s&valueSerde=%s", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(200, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void noKeySerde(TestContext context) {

        Async async = context.async();


        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store?valueSerde=%s", Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(400, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void noValueSerde(TestContext context) {

        Async async = context.async();


        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store?keySerde=%s", Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(400, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void internalServerError(TestContext context){
        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);



        rule.vertx().eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {
            msg.fail(500, "msg");

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store?keySerde=%s&valueSerde=%s", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(500, res.statusCode());
            async.complete();
        }).end();
    }




}
