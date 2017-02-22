package com.github.ftrossbach.kiqr.core.query.facade;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.AllInstancesResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.AllKeyValuesQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.MultiValuedKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.RangeKeyValueQuery;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ftr on 22/02/2017.
 */
public class AllKeyValueQueryFacadeVerticle extends AbstractVerticle{

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {
            AllKeyValuesQuery query = (AllKeyValuesQuery) msg.body();

            vertx.eventBus().send(Config.ALL_INSTANCES, null, reply -> {
                if(reply.succeeded()){

                    AllInstancesResponse response = (AllInstancesResponse) reply.result().body();

                    List<Future> results = response.getInstances().stream()
                            .map(instanceId -> {
                                Future future = Future.future();
                                vertx.eventBus().send(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + instanceId, query);
                                return future;
                            }).collect(Collectors.toList());

                    CompositeFuture all = CompositeFuture.all(results);

                    all.setHandler(handler -> {

                        List<Message<MultiValuedKeyValueQueryResponse>> list = handler.result().list();

                        MultiValuedKeyValueQueryResponse compoundResult = list.stream().map(message -> message.body()).reduce(new MultiValuedKeyValueQueryResponse(), (a, b) -> a.merge(b));
                        msg.reply(compoundResult);
                    });

                } else {
                  msg.fail(-1, reply.cause().getMessage());
                }

            });
        });
    }
}
