package com.github.ftrossbach.kiqr.client.service.rest;


import com.github.ftrossbach.kiqr.rest.server.HttpServer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.http.HttpServerOptions;

/**
 * Created by ftr on 07/03/2017.
 */
public class MockedRuntimeHttpServerVerticle extends HttpServer {
    public MockedRuntimeHttpServerVerticle(HttpServerOptions serverOptions, AbstractVerticle runtimeVerticle) {
        super(serverOptions, runtimeVerticle);
    }
}
