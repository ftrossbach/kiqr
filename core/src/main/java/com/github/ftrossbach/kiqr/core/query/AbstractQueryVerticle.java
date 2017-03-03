package com.github.ftrossbach.kiqr.core.query;

import io.vertx.core.AbstractVerticle;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;

import java.util.Base64;

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

    protected String base64Encode(Serde<Object> serde, Object obj){
        return Base64.getEncoder().encodeToString(serializeObject(serde, obj));
    }







}
