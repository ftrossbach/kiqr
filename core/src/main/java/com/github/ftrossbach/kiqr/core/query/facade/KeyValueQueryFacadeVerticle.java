package com.github.ftrossbach.kiqr.core.query.facade;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import io.vertx.core.AbstractVerticle;

/**
 * Created by ftr on 22/02/2017.
 */
public class KeyValueQueryFacadeVerticle extends AbstractVerticle{

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {
            ScalarKeyValueQuery query = (ScalarKeyValueQuery) msg.body();
            InstanceResolverQuery instanceQuery = new InstanceResolverQuery(query.getStoreName(), query.getKeySerde(), query.getKey() );
            vertx.eventBus().send(Config.INSTANCE_RESOLVER_ADDRESS_SINGLE, instanceQuery, reply -> {
                if(reply.succeeded()){

                    InstanceResolverResponse response = (InstanceResolverResponse) reply.result().body();
                    vertx.eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + response.getInstanceId().get(), query, rep -> {

                        ScalarKeyValueQueryResponse queryResponse = (ScalarKeyValueQueryResponse) rep.result().body();

                        msg.reply(queryResponse);

                    });
                } else {
                  msg.fail(-1, reply.cause().getMessage());
                }

            });
        });
    }
}
