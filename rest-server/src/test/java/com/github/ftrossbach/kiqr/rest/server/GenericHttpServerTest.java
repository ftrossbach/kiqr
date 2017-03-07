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

import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.RunTestOnContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import java.util.Properties;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

/**
 * Created by ftr on 07/03/2017.
 */
@RunWith(VertxUnitRunner.class)
public class GenericHttpServerTest {


    @Rule
    public final RunTestOnContext rule = new RunTestOnContext();

    @Test
    public void deploymentFailure(TestContext context){
        rule.vertx().deployVerticle(new HttpServer(new HttpServerOptions().setPort(5762), new DummyFailingVerticle()), context.asyncAssertFailure());
    }

    @Test
    public void builderWithConfig(){
        HttpServer.Builder builder = new HttpServer.Builder(new KStreamBuilder(), new Properties());

        AbstractVerticle serverVerticle = builder.withOptions(new HttpServerOptions().setPort(4711)).build();
        assertThat(serverVerticle, is(instanceOf(HttpServer.class)));

        HttpServer server = (HttpServer) serverVerticle;
        assertThat(server.serverOptions.getPort(), is(equalTo(4711)));

    }

    @Test
    public void builderWithPort(){
        HttpServer.Builder builder = new HttpServer.Builder(new KStreamBuilder(), new Properties());

        AbstractVerticle serverVerticle = builder.withPort(4711).build();
        assertThat(serverVerticle, is(instanceOf(HttpServer.class)));

        HttpServer server = (HttpServer) serverVerticle;
        assertThat(server.serverOptions.getPort(), is(equalTo(4711)));

    }
}
