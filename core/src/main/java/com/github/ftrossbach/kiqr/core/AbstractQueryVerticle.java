package com.github.ftrossbach.kiqr.core;

import io.vertx.core.AbstractVerticle;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;

/**
 * Created by ftr on 19/02/2017.
 */
public abstract class AbstractQueryVerticle extends AbstractVerticle{

    protected final KafkaStreams streams;
    protected final String instanceId;

    public AbstractQueryVerticle(String instanceId, KafkaStreams streams) {
        this.streams = streams;
        this.instanceId = instanceId;
    }



    protected abstract String getQueryAddressPrefix();


    protected Serde<Object> getSerde(String serde){
        try {
            return (Serde<Object>) Class.forName(serde).newInstance();

        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    protected Object deserializeObject(Serde<Object> serde, byte[] key){
        return serde.deserializer().deserialize("?", key);
    }

    protected byte[] serializeObject(Serde<Object> serde, Object obj) {
        return serde.serializer().serialize("?", obj);
    }





}
