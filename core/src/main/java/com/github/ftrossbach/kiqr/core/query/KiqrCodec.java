/**
 * Copyright © 2017 Florian Troßbach (trossbach@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
