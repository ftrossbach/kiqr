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
package com.github.ftrossbach.kiqr.core.query.facade;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by ftr on 22/02/2017.
 */
public class RangeKeyValueQueryFacadeVerticle extends AbstractVerticle{

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS, msg -> {
            RangeKeyValueQuery query = (RangeKeyValueQuery) msg.body();

            vertx.eventBus().send(Config.ALL_INSTANCES, query.getStoreName(), reply -> {
                if(reply.succeeded()){

                    AllInstancesResponse response = (AllInstancesResponse) reply.result().body();


                    List<Future> results = response.getInstances().stream()
                            .map(instanceId -> {
                                Future<Message<Object>> future = Future.<Message<Object>>future();
                                vertx.eventBus().send(Config.RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX + instanceId, query, future.completer());
                                return future;
                            }).collect(Collectors.toList());

                    CompositeFuture all = CompositeFuture.all(results);



                    all.setHandler(compoundFutureHandler -> {

                        if(compoundFutureHandler.succeeded()) {
                            List<Message<MultiValuedKeyValueQueryResponse>> list = compoundFutureHandler.result().list();

                            MultiValuedKeyValueQueryResponse compoundResult = list.stream().map(message -> message.body()).reduce(new MultiValuedKeyValueQueryResponse(), (a, b) -> a.merge(b));
                            msg.reply(compoundResult);
                        } else {
                            ReplyException cause = (ReplyException) compoundFutureHandler.cause();
                            msg.fail(cause.failureCode(), cause.getMessage());
                        }
                    });



                } else {
                    if(reply.cause() instanceof ReplyException){
                        ReplyException cause = (ReplyException) reply.cause();
                        msg.fail(cause.failureCode(), cause.getMessage());
                    }
                    else {
                        msg.fail(500, reply.cause().getMessage());
                    }

                }

            });
        });
    }
}
