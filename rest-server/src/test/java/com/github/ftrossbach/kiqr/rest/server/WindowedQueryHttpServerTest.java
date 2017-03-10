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
import java.util.*;


import static org.mockito.Mockito.mock;

/**
 * Created by ftr on 06/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class WindowedQueryHttpServerTest {

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();



    @Before
    public void setUp(TestContext context) throws Exception{
        rule.vertx().eventBus().registerDefaultCodec(WindowedQuery.class, new KiqrCodec(WindowedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(WindowedQueryResponse.class, new KiqrCodec<>(WindowedQueryResponse.class));

        rule.vertx().deployVerticle(new RestKiqrServerVerticle(new HttpServerOptions().setPort(5762), new DummySuccessfulVerticle()), context.asyncAssertSuccess());
    }


    @Test
    public void readWindowValue(TestContext context) throws Exception {

        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);



        rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_FACADE_ADDRESS, msg -> {
            context.assertTrue(msg.body() instanceof WindowedQuery);
            WindowedQuery query = (WindowedQuery) msg.body();

            context.assertEquals("store", query.getStoreName());
            context.assertEquals(Serdes.String().getClass().getName(), query.getKeySerde());
            context.assertEquals(Serdes.Long().getClass().getName(), query.getValueSerde());
            context.assertTrue(query.getKey().length > 0);
            context.assertEquals(1L, query.getFrom());
            context.assertEquals(2L, query.getTo());

            SortedMap<Long, String> results = new TreeMap<>();
            results.put(1L, "value1");
            results.put(2L, "value2");

            msg.reply(new WindowedQueryResponse(results));

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/window/store/key?keySerde=%s&valueSerde=%s&from=1&to=2", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(200, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void valuesEmpty(TestContext context) {

        Async async = context.async();


        rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_FACADE_ADDRESS, msg -> {

            msg.reply(new WindowedQueryResponse(Collections.emptySortedMap()));

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/window/store/key?keySerde=%s&valueSerde=%s&from=1&to=2", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(200, res.statusCode());
            async.complete();
        }).end();
    }





    @Test
    public void noKeySerde(TestContext context) {

        Async async = context.async();


        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/window/store/key?valueSerde=%s&from=1&to=2", Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(400, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void noValueSerde(TestContext context) {

        Async async = context.async();


        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/window/store/key?keySerde=%s&from=1&to=2", Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(400, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void internalServerError(TestContext context){
        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);



        rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_FACADE_ADDRESS, msg -> {
            msg.fail(500, "msg");

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/window/store/key?keySerde=%s&valueSerde=%s&from=1&to=2", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(500, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void noFrom(TestContext context){
        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);


        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/window/store/key?keySerde=%s&valueSerde=%s&to=2", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(400, res.statusCode());
            async.complete();
        }).end();
    }

    @Test
    public void noTo(TestContext context){
        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);


        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/window/store/key?keySerde=%s&valueSerde=%s&from=1", Serdes.String().getClass().getName(), Serdes.Long().getClass().getName()), res ->{

            context.assertEquals(400, res.statusCode());
            async.complete();
        }).end();
    }




}
