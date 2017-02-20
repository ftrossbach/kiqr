package com.github.ftrossbach.kiqr.commons.config.querymodel.codec;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.io.ByteArrayOutputStream;

/**
 * Created by ftr on 20/02/2017.
 */
public abstract class AbstractCodec<T> implements MessageCodec<T, T> {

    private final Kryo kryo;

    public AbstractCodec(){
        kryo = new Kryo();
        kryo.register(getObjectClass(), getObjectId());
    }

    protected abstract int getObjectId();

    @Override
    public void encodeToWire(Buffer buffer, T object) {

        ByteArrayOutputStream outputStream = new ByteArrayOutputStream(100);
        kryo.writeObject(new ByteBufferOutput(outputStream), object);
        buffer.appendBytes(outputStream.toByteArray());
    }

    @Override
    public T decodeFromWire(int pos, Buffer buffer) {

        byte[] bytes = buffer.getBytes(pos, buffer.length());
        return kryo.readObject(new ByteBufferInput(bytes), getObjectClass());
    }

    protected abstract Class<T> getObjectClass();


    @Override
    public T transform(T object) {
        return object;
    }

    @Override
    public String name() {
        return getClass().getName();
    }

    @Override
    public byte systemCodecID() {
        return -1;
    }
}
