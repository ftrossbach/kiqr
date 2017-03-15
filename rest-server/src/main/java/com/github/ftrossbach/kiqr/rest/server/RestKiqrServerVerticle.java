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
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Base64;
import java.util.Properties;

/**
 * Created by ftr on 28/02/2017.
 */
public class RestKiqrServerVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(RestKiqrServerVerticle.class);
    public static final int TIMEOUT = 5000;

    static String BASE_ROUTE_KV = "/api/v1/kv/:store";
    static String BASE_ROUTE_WINDOW = "/api/v1/window/:store";

    public static class Builder extends RuntimeVerticle.Builder<RestKiqrServerVerticle.Builder>{

        private HttpServerOptions httpServerOptions;

        public static Builder serverBuilder(KStreamBuilder builder){
            return new Builder(builder);
        }

        public static Builder serverBuilder(KStreamBuilder builder, Properties props){
            return new Builder(builder, props);
        }
        protected Builder(KStreamBuilder builder) {
            super(builder);
        }

        protected Builder(KStreamBuilder builder, Properties properties) {
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
            return new RestKiqrServerVerticle(httpServerOptions != null ? httpServerOptions : new HttpServerOptions(), runtimeVerticle);
        }
    }

    protected final HttpServerOptions serverOptions;
    protected final AbstractVerticle runtimeVerticle;

    protected RestKiqrServerVerticle(HttpServerOptions serverOptions, AbstractVerticle runtimeVerticle) {
        this.serverOptions = serverOptions;
        this.runtimeVerticle = runtimeVerticle;
    }

    @Override
    public void start(Future<Void> fut) throws Exception {

        LOG.info("Starting KafkaStreams and Webserver");
        Future runtimeVerticleCompleter = Future.future();
        vertx.deployVerticle(runtimeVerticle, runtimeVerticleCompleter.completer());

        // Create a router object.
        Router router = Router.router(vertx);

        addRouteForMultiValuedKVQueries(router);
        addRouteForScalarKVQueries(router);

        addRouteForWindowQueries(router);

        Future serverListener = Future.future();

        int port = vertx
                .createHttpServer(serverOptions)
                .requestHandler(router::accept)
                .listen(serverListener.completer())
                .actualPort();


        CompositeFuture.all(runtimeVerticleCompleter, serverListener).setHandler(handler ->{
           if(handler.succeeded()){
               LOG.info("Started KafkaStreams and Webserver, listening on port " + port);
               fut.complete();
           } else {
               LOG.error("Failure during startup", handler.cause());
               fut.fail(handler.cause());
           }
        });
    }



    private void addRouteForWindowQueries(Router router) {
        router.route(RestKiqrServerVerticle.BASE_ROUTE_WINDOW + "/:key").handler(routingContext -> {

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



                vertx.eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, new DeliveryOptions().setSendTimeout(TIMEOUT), reply -> {
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
        router.route(RestKiqrServerVerticle.BASE_ROUTE_KV + "/:key").handler(routingContext -> {


            HttpServerRequest request = routingContext.request();

            String keySerde = request.getParam("keySerde");
            String valueSerde = request.getParam("valueSerde");
            String store = request.getParam("store");
            byte[] key = Base64.getDecoder().decode(request.getParam("key"));

            if(keySerde == null || valueSerde == null){
                routingContext.fail(400);
            } else {
                ScalarKeyValueQuery query = new ScalarKeyValueQuery(store, keySerde, key, valueSerde);


                vertx.eventBus().send(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, query, new DeliveryOptions().setSendTimeout(TIMEOUT), reply -> {
                    if(reply.succeeded()){
                        ScalarKeyValueQueryResponse body = (ScalarKeyValueQueryResponse) reply.result().body();
                        HttpServerResponse response = routingContext.response();
                        System.out.println(reply.result().body());
                        response
                                .putHeader("content-type", "application/json")
                                .end(JsonObject.mapFrom(reply.result().body()).encode());


                    } else {

                        reply.cause().printStackTrace();
                        forwardErrorCode(routingContext, reply);
                    }
                });
            }


        });
    }

    private void forwardErrorCode(RoutingContext routingContext, AsyncResult<Message<Object>> reply) {
        ReplyException ex = (ReplyException) reply.cause();
        ex.printStackTrace();
        HttpServerResponse response = routingContext.response();
        response.setStatusCode(ex.failureCode());
        response.end();
    }

    private void addRouteForMultiValuedKVQueries(Router router) {
        router.route(RestKiqrServerVerticle.BASE_ROUTE_KV).handler(routingContext -> {


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

                    query = new StoreWideQuery(store, keySerde, valueSerde);
                    address = Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS;
                } else {
                    byte[] fromArray = Base64.getDecoder().decode(from);
                    byte[] toArray = Base64.getDecoder().decode(to);

                    query = new RangeKeyValueQuery(store, keySerde, valueSerde, fromArray, toArray);
                    address = Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS;

                }

                vertx.eventBus().send(address, query, new DeliveryOptions().setSendTimeout(TIMEOUT), reply -> {
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
