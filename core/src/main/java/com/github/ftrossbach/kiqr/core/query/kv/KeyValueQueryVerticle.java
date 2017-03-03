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
package com.github.ftrossbach.kiqr.core.query.kv;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.QueryStatus;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Created by ftr on 19/02/2017.
 */
public class KeyValueQueryVerticle extends AbstractQueryVerticle {


    public KeyValueQueryVerticle(String instanceId, KafkaStreams streams) {
        super(instanceId, streams);
    }



    @Override
    public void start() throws Exception {

        vertx.eventBus().consumer(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + instanceId, msg -> {

            ScalarKeyValueQuery query = (ScalarKeyValueQuery) msg.body();

            Serde<Object> keySerde = getSerde(query.getKeySerde());
            Serde<Object> valueSerde = getSerde(query.getValueSerde());

            Object deserializedKey = deserializeObject(keySerde, query.getKey());
            ReadOnlyKeyValueStore<Object, Object> kvStore = streams.store(query.getStoreName(), QueryableStoreTypes.keyValueStore());
            Object result = kvStore.get(deserializedKey);
            if (result != null) {
                msg.reply(new ScalarKeyValueQueryResponse(QueryStatus.OK, base64Encode(valueSerde, result)));
            } else {
                msg.reply(new ScalarKeyValueQueryResponse(QueryStatus.NOT_FOUND, null));
            }

        });

    }


}
