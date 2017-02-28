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
public class WindowRestEndpoint extends AbstractVerticle {
    private final Router router;

    public WindowRestEndpoint(Router router) {
        this.router = router;
    }

    @Override
    public void start() throws Exception {
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
}
