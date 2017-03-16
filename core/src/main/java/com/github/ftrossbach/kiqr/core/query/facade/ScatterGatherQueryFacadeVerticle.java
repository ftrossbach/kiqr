/**
 * Copyright © 2017 Florian Troßbach (trossbach@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ftrossbach.kiqr.core.query.facade;

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.ShareableStreamsMetadataProvider;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.ReplyException;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Collection;
import java.util.List;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by ftr on 22/02/2017.
 */
public class ScatterGatherQueryFacadeVerticle<RES> extends AbstractVerticle {

    private final String listeningAddress;
    private final String queryAddressPrefix;
    private final Supplier<RES> identity;
    private final BinaryOperator<RES> reducer;

    public ScatterGatherQueryFacadeVerticle(String listeningAddress, String queryAddressPrefix, Supplier<RES> identity, BinaryOperator<RES> reducer) {
        this.listeningAddress = listeningAddress;
        this.queryAddressPrefix = queryAddressPrefix;
        this.identity = identity;
        this.reducer = reducer;
    }

    @Override
    public void start() throws Exception {
        vertx.eventBus().consumer(listeningAddress, msg -> {
            HasStoreName query = (HasStoreName) msg.body();


            ShareableStreamsMetadataProvider metadataProvider = (ShareableStreamsMetadataProvider) vertx.sharedData().getLocalMap("metadata").get("metadata");

            Collection<StreamsMetadata> allMetadata = metadataProvider.allMetadataForStore(query.getStoreName());


            if (allMetadata.isEmpty()) {
                msg.fail(404, "No instance for store found: " + query.getStoreName());
            } else if (allMetadata.stream().anyMatch(meta -> "unavailable".equals(meta.host()))) {
                msg.fail(503, "Streaming application currently unavailable");
            }


            List<Future> results = allMetadata.stream().map(StreamsMetadata::host)
                    .map(instanceId -> {
                        Future<Message<Object>> future = Future.<Message<Object>>future();
                        vertx.eventBus().send(queryAddressPrefix + instanceId, query, future.completer());
                        return future;
                    }).collect(Collectors.toList());

            CompositeFuture all = CompositeFuture.all(results);


            all.setHandler(compoundFutureHandler -> {

                if (compoundFutureHandler.succeeded()) {
                    List<Message<RES>> list = compoundFutureHandler.result().list();

                    RES reduced = list.stream().map(message -> message.body()).reduce(identity.get(), reducer);
                    msg.reply(reduced);
                } else {
                    ReplyException cause = (ReplyException) compoundFutureHandler.cause();
                    msg.fail(cause.failureCode(), cause.getMessage());
                }
            });


        });

    }
}

