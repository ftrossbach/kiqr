package com.github.ftrossbach.kiqr.core.query;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.codecs.JsonObjectMessageCodec;
import io.vertx.core.eventbus.impl.codecs.StringMessageCodec;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

/**
 * Created by ftr on 20/02/2017.
 */
public class KiqrCodec<T> implements MessageCodec<T, T> {

    private final JsonObjectMessageCodec codec = new JsonObjectMessageCodec();

    private final Class<T> clazz;

    public KiqrCodec(Class<T> clazz){
        this.clazz = clazz;
    }


    @Override
    public void encodeToWire(Buffer buffer, T object) {
        //ToDo: more efficient serialization than JSON for internal purposes
        codec.encodeToWire(buffer, JsonObject.mapFrom(object));
    }

    @Override
    public T decodeFromWire(int pos, Buffer buffer) {
        //ToDo: more efficient deserialization

       return codec.decodeFromWire(pos, buffer).mapTo(clazz);
    }


    @Override
    public T transform(T object) {
        return object;
    }

    @Override
    public String name() {
        return clazz.getName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
