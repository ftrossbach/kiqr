package com.github.ftrossbach.kiqr.rest.server;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;

import java.util.Base64;

/**
 * Created by ftr on 28/02/2017.
 */
public class MultiValuedKeyValueRestEndpoint extends AbstractVerticle {

    private final Router router;

    public MultiValuedKeyValueRestEndpoint(Router router) {
        this.router = router;
    }

    @Override
    public void start() throws Exception {

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
