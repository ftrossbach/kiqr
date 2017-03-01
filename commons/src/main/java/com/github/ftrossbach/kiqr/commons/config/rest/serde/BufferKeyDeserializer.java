package com.github.ftrossbach.kiqr.commons.config.rest.serde;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import io.vertx.core.buffer.Buffer;

import java.io.IOException;
import java.util.Base64;

/**
 * Created by ftr on 01/03/2017.
 */
public class BufferKeyDeserializer extends KeyDeserializer {


    @Override
    public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        return Buffer.buffer(Base64.getDecoder().decode(key));
    }
}
