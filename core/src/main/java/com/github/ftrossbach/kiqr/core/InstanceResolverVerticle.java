package com.github.ftrossbach.kiqr.core;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.QueryStatus;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonObject;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.StreamsMetadata;

import java.util.Base64;
import java.util.Optional;

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

        vertx.eventBus().localConsumer(Config.INSTANCE_RESOLVER_ADDRESS, msg -> {
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

    }
}
