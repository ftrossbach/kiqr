package com.github.ftrossbach.kiqr.core.query;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.AllInstancesResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.QueryStatus;
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
                throw new RuntimeException(e);
            }

            Object deserializedKey = serde.deserializer().deserialize("?", config.getKey());
            StreamsMetadata streamsMetadata = streams.metadataForKey(config.getStoreName(), deserializedKey, serde.serializer());

            if(streamsMetadata.host() != null){
                msg.reply(new InstanceResolverResponse(QueryStatus.OK, Optional.of(streamsMetadata.host())));
            } else {
                msg.reply(new InstanceResolverResponse(QueryStatus.NOT_FOUND, Optional.empty()));
            }


        });


        vertx.eventBus().consumer(Config.ALL_INSTANCES, msg -> {

            Set<String> instances = streams.allMetadata().stream().map(metadata -> metadata.host()).collect(Collectors.toSet());

            msg.reply(new AllInstancesResponse(instances));


        });
    }
}
