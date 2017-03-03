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
package com.github.ftrossbach.kiqr.core.query;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.AllInstancesResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.QueryStatus;
import com.github.ftrossbach.kiqr.core.query.exceptions.SerdeNotFoundException;
import io.vertx.core.AbstractVerticle;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by ftr on 19/02/2017.
 */
public class InstanceResolverVerticle extends AbstractVerticle {

    private final KafkaStreams streams;

    public InstanceResolverVerticle(KafkaStreams streams) {
        if(streams == null) throw new IllegalArgumentException("Streams must not be null");
        this.streams = streams;
    }

    @Override
    public void start() throws Exception {

        vertx.eventBus().consumer(Config.INSTANCE_RESOLVER_ADDRESS_SINGLE, msg -> {




            InstanceResolverQuery config = (InstanceResolverQuery) msg.body();

            Serde<Object> serde = null;
            try {
               serde = (Serde<Object>) Class.forName(config.getKeySerde()).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                throw new SerdeNotFoundException(e);
            }

            Object deserializedKey = serde.deserializer().deserialize("?", config.getKey());
            StreamsMetadata streamsMetadata = streams.metadataForKey(config.getStoreName(), deserializedKey, serde.serializer());


            if(streamsMetadata != null && streamsMetadata.host() != null){
                msg.reply(new InstanceResolverResponse(QueryStatus.OK, Optional.of(streamsMetadata.host())));
            } else {
                msg.fail(404, "No instance for store found: " + config.getStoreName());
            }


        });


        vertx.eventBus().consumer(Config.ALL_INSTANCES, msg -> {


            try {
                String store = (String) msg.body();
                Set<String> instances = streams.allMetadataForStore(store).stream().map(metadata -> metadata.host()).collect(Collectors.toSet());

                msg.reply(new AllInstancesResponse(instances));
            } catch(RuntimeException e){
                msg.fail(500, e.getMessage());
            }




        });
    }
}
