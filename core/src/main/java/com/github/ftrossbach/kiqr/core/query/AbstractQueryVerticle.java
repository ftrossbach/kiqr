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


    @Override
    public void stop() throws Exception {
        streams.close();
    }




}
