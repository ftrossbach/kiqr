package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.awt.*;
import java.io.ByteArrayOutputStream;

/**
 * Created by ftr on 20/02/2017.
 */
public  class KiqrCodec<T> implements MessageCodec<T, T> {

    private final Class<T> clazz;

    private static final ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            // configure kryo instance, customize settings
            kryo.register(AllKeyValuesQuery.class);
            kryo.register(InstanceResolverQuery.class);
            kryo.register(InstanceResolverResponse.class);
            kryo.register(MultiValuedKeyValueQueryResponse.class);
            kryo.register(RangeKeyValueQuery.class);
            kryo.register(ScalarKeyValueQuery.class);
            kryo.register(ScalarKeyValueQueryResponse.class);
            kryo.register(WindowedQuery.class);
            kryo.register(WindowedQueryResponse.class);

            return kryo;
        };
    };

    public KiqrCodec(Class<T> clazz){
        this.clazz = clazz;
    }


    @Override
    public void encodeToWire(Buffer buffer, T object) {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(100);
        kryos.get().writeObject(new ByteBufferOutput(outputStream), object);
        buffer.appendBytes(outputStream.toByteArray());
    }

    @Override
    public T decodeFromWire(int pos, Buffer buffer) {

        byte[] bytes = buffer.getBytes(pos, buffer.length());
        return kryos.get().readObject(new ByteBufferInput(bytes), clazz);
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
