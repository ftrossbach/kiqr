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
public class CountQueryHttpServerTest {

    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();



    @Before
    public void setUp(TestContext context) throws Exception{
        rule.vertx().eventBus().registerDefaultCodec(StoreWideQuery.class, new KiqrCodec(StoreWideQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(KeyValueStoreCountQuery.class, new KiqrCodec<>(KeyValueStoreCountQuery.class));


        rule.vertx().deployVerticle(new RestKiqrServerVerticle(new HttpServerOptions().setPort(5762), new DummySuccessfulVerticle()), context.asyncAssertSuccess());
    }


    @Test
    public void readCount(TestContext context) throws Exception {

        Async async = context.async();

        RuntimeVerticle mock = mock(RuntimeVerticle.class);



        rule.vertx().eventBus().consumer(Config.COUNT_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {
            context.assertTrue(msg.body() instanceof HasStoreName);
            HasStoreName query = (HasStoreName) msg.body();

            context.assertEquals("store", query.getStoreName());


            msg.reply(42L);

        });

        rule.vertx().createHttpClient().get(5762, "localhost", String.format("/api/v1/kv/store/count"), res ->{

            context.assertEquals(200, res.statusCode());
            async.complete();
        }).end();
    }






}
