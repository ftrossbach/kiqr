package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.eventbus.impl.codecs.StringMessageCodec;
import io.vertx.core.json.Json;

/**
 * Created by ftr on 20/02/2017.
 */
public class KiqrCodec<T> implements MessageCodec<T, T> {

    private final StringMessageCodec codec = new StringMessageCodec();

    private final Class<T> clazz;

    public KiqrCodec(Class<T> clazz){
        this.clazz = clazz;
    }


    @Override
    public void encodeToWire(Buffer buffer, T object) {
        //ToDo: more efficient serialization than JSON for internal purposes

        codec.encodeToWire(buffer, Json.encode(object));
    }

    @Override
    public T decodeFromWire(int pos, Buffer buffer) {
        //ToDo: more efficient deserialization

       return Json.decodeValue(codec.decodeFromWire(pos, buffer), clazz);
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
