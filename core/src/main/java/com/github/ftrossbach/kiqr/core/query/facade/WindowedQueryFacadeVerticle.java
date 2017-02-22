package com.github.ftrossbach.kiqr.core.query.facade;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import io.vertx.core.AbstractVerticle;

/**
 * Created by ftr on 22/02/2017.
 */
public class WindowedQueryFacadeVerticle extends AbstractVerticle{

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(Config.WINDOWED_QUERY_FACADE_ADDRESS, msg -> {
            WindowedQuery query = (WindowedQuery) msg.body();
            InstanceResolverQuery instanceQuery = new InstanceResolverQuery(query.getStoreName(), query.getKeySerde(), query.getKey() );
            vertx.eventBus().send(Config.INSTANCE_RESOLVER_ADDRESS_SINGLE, instanceQuery, reply -> {
                if(reply.succeeded()){

                    InstanceResolverResponse response = (InstanceResolverResponse) reply.result().body();
                    vertx.eventBus().send(Config.WINDOWED_QUERY_ADDRESS_PREFIX + response.getInstanceId().get(), query, rep -> {

                        if(rep.failed()){
                            rep.cause().printStackTrace();
                            msg.fail(-1, rep.cause().getMessage());
                        }
                        else {
                            WindowedQueryResponse queryResponse = (WindowedQueryResponse) rep.result().body();
                            msg.reply(queryResponse);
                        }


                    });
                } else {
                  msg.fail(-1, reply.cause().getMessage());
                }

            });
        });
    }
}
