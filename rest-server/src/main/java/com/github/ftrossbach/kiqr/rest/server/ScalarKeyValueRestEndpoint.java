package com.github.ftrossbach.kiqr.rest.server;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.Json;
import io.vertx.ext.web.Router;

import java.util.Base64;

/**
 * Created by ftr on 28/02/2017.
 */
public class ScalarKeyValueRestEndpoint extends AbstractVerticle {

    private final Router router;

    public ScalarKeyValueRestEndpoint(Router router) {
        this.router = router;
    }

    @Override
    public void start() throws Exception {

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
}
