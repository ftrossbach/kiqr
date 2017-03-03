package com.github.ftrossbach.kiqr.rest.server;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;

import java.util.Base64;

/**
 * Created by ftr on 28/02/2017.
 */
public class HttpServer extends AbstractVerticle {

    static String BASE_ROUTE_KV = "/api/v1/kv/:store";
    static String BASE_ROUTE_WINDOW = "/api/v1/window/:store";

    private final HttpServerOptions serverOptions;

    public HttpServer(HttpServerOptions serverOptions) {
        this.serverOptions = serverOptions;
    }

    @Override
    public void start(Future<Void> fut) throws Exception {
        // Create a router object.
        Router router = Router.router(vertx);

        addRouteForMultiValuedKVQueries(router);
        addRouteForScalarKVQueries(router);

        addRouteForWindowQueries(router);


        vertx
                .createHttpServer(serverOptions)
                .requestHandler(router::accept)
                .listen(
                        result -> {
                            if (result.succeeded()) {
                                fut.complete();
                            } else {
                                fut.fail(result.cause());
                            }
                        }
                );


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
                routingContext.fail(404);
            }



            if (from == null && to == null) {
                routingContext.fail(404);
            }
            WindowedQuery query = new WindowedQuery(store, keySerde, key,valueSerde, Long.valueOf(from), Long.valueOf(to));



            vertx.eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, query, reply -> {
                if (reply.succeeded()) {
                    WindowedQueryResponse body = (WindowedQueryResponse) reply.result().body();

                    HttpServerResponse response = routingContext.response();
                    response
                            .putHeader("content-type", "application/json")
                            .end(Json.encode(body));


                }
            });
        });
    }

    private void addRouteForScalarKVQueries(Router router) {
        router.route(HttpServer.BASE_ROUTE_KV + "/:key").handler(routingContext -> {


            HttpServerRequest request = routingContext.request();

            String keySerde = request.getParam("keySerde");
            String valueSerde = request.getParam("valueSerde");
            String store = request.getParam("store");
            System.out.println(request.absoluteURI());
            byte[] key = Base64.getDecoder().decode(request.getParam("key"));

            if(keySerde == null || valueSerde == null){
                routingContext.fail(404);
            }

            ScalarKeyValueQuery query = new ScalarKeyValueQuery(store, keySerde, key, valueSerde);


            vertx.eventBus().send(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, query, reply -> {
                if(reply.succeeded()){
                    ScalarKeyValueQueryResponse body = (ScalarKeyValueQueryResponse) reply.result().body();
                    HttpServerResponse response = routingContext.response();
                    response
                            .putHeader("content-type", "application/json")
                            .end(Json.encode(body));


                }
            });


        });
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
                routingContext.fail(404);
            }

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


                }
            });
        });
    }


}
