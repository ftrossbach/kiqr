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
import com.github.ftrossbach.kiqr.core.ShareableStreamsMetadataProvider;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.StreamsMetadata;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.Collections;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;


import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Created by ftr on 06/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class WindowQueryFacadeVerticleTest {

    @Rule
    public RunTestOnContext rule = new RunTestOnContext();

    @Before
    public void setUp(){
        rule.vertx().eventBus().registerDefaultCodec(WindowedQuery.class, new KiqrCodec(WindowedQuery.class));
        rule.vertx().eventBus().registerDefaultCodec(WindowedQueryResponse.class, new KiqrCodec(WindowedQueryResponse.class));

    }

    @Test
    public void success(TestContext context){

        ShareableStreamsMetadataProvider mock = mock(ShareableStreamsMetadataProvider.class);
        StreamsMetadata host = new StreamsMetadata(new HostInfo("host", 1), Collections.emptySet(), Collections.emptySet());
        when(mock.metadataForKey(anyString(), any(), any(Serializer.class))).thenReturn(host);
        rule.vertx().sharedData().getLocalMap("metadata").put("metadata", mock);

        rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", msg -> {
            SortedMap<Long, String> result = new TreeMap<>();

            result.put(1L, "abc");
            result.put(2L, "def");
            msg.reply(new WindowedQueryResponse(result ));
        });

        rule.vertx().deployVerticle(new KeyBasedQueryFacadeVerticle<WindowedQuery, WindowedQueryResponse>(Config.WINDOWED_QUERY_FACADE_ADDRESS, Config.WINDOWED_QUERY_ADDRESS_PREFIX), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),Serdes.String().getClass().getName(),  1, 2);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, context.asyncAssertSuccess(reply ->{

                context.assertTrue(reply.body() instanceof WindowedQueryResponse);
                WindowedQueryResponse response = (WindowedQueryResponse) reply.body();
                context.assertEquals(2, response.getValues().size());
                context.assertTrue(response.getValues().containsKey(1L));
                context.assertEquals("abc", response.getValues().get(1L));
                context.assertTrue(response.getValues().containsKey(2L));
                context.assertEquals("def", response.getValues().get(2L));

            }));


        }));

    }

    @Test
    public void forwardingFailureDuringQuery(TestContext context){


        ShareableStreamsMetadataProvider mock = mock(ShareableStreamsMetadataProvider.class);
        StreamsMetadata host = new StreamsMetadata(new HostInfo("host", 1), Collections.emptySet(), Collections.emptySet());
        when(mock.metadataForKey(anyString(), any(), any(Serializer.class))).thenReturn(host);
        rule.vertx().sharedData().getLocalMap("metadata").put("metadata", mock);


        rule.vertx().eventBus().consumer(Config.WINDOWED_QUERY_ADDRESS_PREFIX + "host", msg -> {
            msg.fail(400, "msg");
        });

        rule.vertx().deployVerticle(new KeyBasedQueryFacadeVerticle<WindowedQuery, WindowedQueryResponse>(Config.WINDOWED_QUERY_FACADE_ADDRESS, Config.WINDOWED_QUERY_ADDRESS_PREFIX), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),Serdes.String().getClass().getName(),  1, 2);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(400, ex.failureCode());
                context.assertEquals("msg", ex.getMessage());

            }));
        }));

    }

    @Test
    public void forwardingFailureDuringInstanceLookup(TestContext context){

        ShareableStreamsMetadataProvider mock = mock(ShareableStreamsMetadataProvider.class);
        StreamsMetadata host = new StreamsMetadata(new HostInfo("host", 1), Collections.emptySet(), Collections.emptySet());
        when(mock.metadataForKey(anyString(), any(), any(Serializer.class))).thenReturn(null);
        rule.vertx().sharedData().getLocalMap("metadata").put("metadata", mock);


        rule.vertx().deployVerticle(new KeyBasedQueryFacadeVerticle<WindowedQuery, WindowedQueryResponse>(Config.WINDOWED_QUERY_FACADE_ADDRESS, Config.WINDOWED_QUERY_ADDRESS_PREFIX), context.asyncAssertSuccess(deployment->{

            WindowedQuery query = new WindowedQuery("store", Serdes.String().getClass().getName(), "key".getBytes(),Serdes.String().getClass().getName(),  1, 2);

            rule.vertx().eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, context.asyncAssertFailure(handler ->{

                context.assertTrue(handler instanceof ReplyException);
                ReplyException ex = (ReplyException) handler;
                context.assertEquals(404, ex.failureCode());

            }));
        }));

    }
}
