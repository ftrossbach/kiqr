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
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Base64;
import java.util.Properties;

/**
 * Created by ftr on 28/02/2017.
 */
public class HttpServer extends AbstractVerticle {

    static String BASE_ROUTE_KV = "/api/v1/kv/:store";
    static String BASE_ROUTE_WINDOW = "/api/v1/window/:store";

    public static class Builder extends RuntimeVerticle.Builder{


        private HttpServerOptions httpServerOptions;

        public Builder(KStreamBuilder builder) {
            super(builder);
        }

        public Builder(KStreamBuilder builder, Properties properties) {
            super(builder, properties);
        }

        public Builder withOptions(HttpServerOptions options) {
            this.httpServerOptions = options;
            return this;
        }

        public Builder withPort(int port) {
            this.httpServerOptions = new HttpServerOptions().setPort(port);
            return this;
        }

        @Override
        public AbstractVerticle build() {
            AbstractVerticle runtimeVerticle = super.build();
            return new HttpServer(httpServerOptions != null ? httpServerOptions : new HttpServerOptions(), runtimeVerticle);
        }
    }

    protected final HttpServerOptions serverOptions;
    protected final AbstractVerticle runtimeVerticle;

    protected HttpServer(HttpServerOptions serverOptions, AbstractVerticle runtimeVerticle) {
        this.serverOptions = serverOptions;
        this.runtimeVerticle = runtimeVerticle;
    }

    @Override
    public void start(Future<Void> fut) throws Exception {

        Future runtimeVerticleCompleter = Future.future();
        vertx.deployVerticle(runtimeVerticle, runtimeVerticleCompleter.completer());

        // Create a router object.
        Router router = Router.router(vertx);

        addRouteForMultiValuedKVQueries(router);
        addRouteForScalarKVQueries(router);

        addRouteForWindowQueries(router);

        Future serverListener = Future.future();

        vertx
                .createHttpServer(serverOptions)
                .requestHandler(router::accept)
                .listen(serverListener.completer());


        CompositeFuture.all(runtimeVerticleCompleter, serverListener).setHandler(handler ->{
           if(handler.succeeded()){
               fut.complete();
           } else {
               fut.fail(handler.cause());
           }
        });
    }

    private void addRouteForWindowQueries(Router router) {
        router.route(HttpServer.BASE_ROUTE_WINDOW + "/:key").handler(routingContext -> {


            HttpServerRequest request = routingContext.request();

            String keySerde = request.getParam("keySerde");
            String valueSerde = request.getParam("valueSerde");
            String store = request.getParam("store");
            byte[] key = Base64.getDecoder().decode(request.getParam("key"));
            String from = request.getParam("from");
            String to = request.getParam("to");

            if (keySerde == null || valueSerde == null) {
                routingContext.fail(400);
            }
            else if (from == null || to == null) {
                routingContext.fail(400);
            } else {
                WindowedQuery query = new WindowedQuery(store, keySerde, key,valueSerde, Long.valueOf(from), Long.valueOf(to));



                vertx.eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, reply -> {
                    if (reply.succeeded()) {
                        WindowedQueryResponse body = (WindowedQueryResponse) reply.result().body();

                        HttpServerResponse response = routingContext.response();
                        response
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(body));


                    } else {
                        forwardErrorCode(routingContext, reply);
                    }
                });
            }

        });
    }

    private void addRouteForScalarKVQueries(Router router) {
        router.route(HttpServer.BASE_ROUTE_KV + "/:key").handler(routingContext -> {


            HttpServerRequest request = routingContext.request();

            String keySerde = request.getParam("keySerde");
            String valueSerde = request.getParam("valueSerde");
            String store = request.getParam("store");
            byte[] key = Base64.getDecoder().decode(request.getParam("key"));

            if(keySerde == null || valueSerde == null){
                routingContext.fail(400);
            } else {
                ScalarKeyValueQuery query = new ScalarKeyValueQuery(store, keySerde, key, valueSerde);


                vertx.eventBus().send(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, query, reply -> {
                    if(reply.succeeded()){
                        ScalarKeyValueQueryResponse body = (ScalarKeyValueQueryResponse) reply.result().body();
                        HttpServerResponse response = routingContext.response();
                        response
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(body));


                    } else {

                        forwardErrorCode(routingContext, reply);
                    }
                });
            }


        });
    }

    private void forwardErrorCode(RoutingContext routingContext, AsyncResult<Message<Object>> reply) {
        ReplyException ex = (ReplyException) reply.cause();
        HttpServerResponse response = routingContext.response();
        response.setStatusCode(ex.failureCode());
        response.end();
    }

    private void addRouteForMultiValuedKVQueries(Router router) {
        router.route(HttpServer.BASE_ROUTE_KV).handler(routingContext -> {


            HttpServerRequest request = routingContext.request();

            String keySerde = request.getParam("keySerde");
            String valueSerde = request.getParam("valueSerde");
            String store = request.getParam("store");
            String from = request.getParam("from");
            String to = request.getParam("to");

            if (keySerde == null || valueSerde == null) {
                routingContext.fail(400);
            } else {
                Object query;
                String address;

                if (from == null && to == null) {

                    query = new AllKeyValuesQuery(store, keySerde, valueSerde);
                    address = Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS;
                } else {
                    byte[] fromArray = Base64.getDecoder().decode(from);
                    byte[] toArray = Base64.getDecoder().decode(to);

                    query = new RangeKeyValueQuery(store, keySerde, valueSerde, fromArray, toArray);
                    address = Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS;

                }

                vertx.eventBus().send(address, query, reply -> {
                    if (reply.succeeded()) {
                        MultiValuedKeyValueQueryResponse body = (MultiValuedKeyValueQueryResponse) reply.result().body();

                        HttpServerResponse response = routingContext.response();
                        response
                                .putHeader("content-type", "application/json")
                                .end(Json.encode(body));


                    } else {
                        forwardErrorCode(routingContext, reply);
                    }
                });
            }


        });
    }


}
