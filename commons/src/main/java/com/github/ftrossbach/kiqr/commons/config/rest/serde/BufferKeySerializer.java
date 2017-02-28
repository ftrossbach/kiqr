package com.github.ftrossbach.kiqr.commons.config.rest.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import io.vertx.core.buffer.Buffer;

import java.io.IOException;
import java.util.Base64;

/**
 * Created by ftr on 28/02/2017.
 */
public class BufferKeySerializer extends StdSerializer<Buffer>{

    protected BufferKeySerializer() {
        super(Buffer.class);
    }

    @Override
    public void serialize(Buffer value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeFieldName(Base64.getEncoder().encodeToString(value.getBytes()));
    }
}
