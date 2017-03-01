package com.github.ftrossbach.kiqr.commons.config.rest.serde;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.vertx.core.buffer.Buffer;

import java.io.IOException;
import java.util.Base64;

/**
 * Created by ftr on 01/03/2017.
 */
public class BufferValueDeserializer extends StdDeserializer<Buffer> {

    public BufferValueDeserializer(){
        super(Buffer.class);
    }


    @Override
    public Buffer deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {

        String value = p.getValueAsString();

        byte[] bytes = Base64.getDecoder().decode(value);

        return Buffer.buffer(bytes);
    }
}
