package com.github.ftrossbach.kiqr.rest.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.ext.web.Router;

/**
 * Created by ftr on 28/02/2017.
 */
public class HttpServer extends AbstractVerticle {

    static String BASE_ROUTE_KV = "/api/v1/kv/:store";
    static String BASE_ROUTE_WINDOW = "/api/v1/window/:store";

    @Override
    public void start(Future<Void> fut) throws Exception {
        // Create a router object.
        Router router = Router.router(vertx);



        vertx.deployVerticle(new ScalarKeyValueRestEndpoint(router));
        vertx.deployVerticle(new MultiValuedKeyValueRestEndpoint(router));
        vertx.deployVerticle(new WindowRestEndpoint(router));
        // Create the HTTP server and pass the "accept" method to the request handler.
        vertx
                .createHttpServer()
                .requestHandler(router::accept)
                .listen(
                        // Retrieve the port from the configuration,
                        // default to 8080.
                        config().getInteger("http.port", 2901),
                        result -> {
                            if (result.succeeded()) {
                                fut.complete();
                            } else {
                                fut.fail(result.cause());
                            }
                        }
                );


    }

}
