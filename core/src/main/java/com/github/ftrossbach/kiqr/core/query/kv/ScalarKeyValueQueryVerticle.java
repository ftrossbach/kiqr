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
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.KeyBasedQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.exceptions.ScalarValueNotFoundException;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Created by ftr on 19/02/2017.
 */
public class ScalarKeyValueQueryVerticle extends AbstractQueryVerticle {


    public ScalarKeyValueQueryVerticle(String instanceId, KafkaStreams streams) {
        super(instanceId, streams);
    }



    @Override
    public void start() throws Exception {

        execute(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX, (abstractQuery, keySerde, valueSerde) -> {
            KeyBasedQuery query = (KeyBasedQuery) abstractQuery;

            Object deserializedKey = deserializeObject(keySerde, query.getKey());
            ReadOnlyKeyValueStore<Object, Object> kvStore = streams.store(query.getStoreName(), QueryableStoreTypes.keyValueStore());
            Object result = kvStore.get(deserializedKey);
            if (result != null) {
                return new ScalarKeyValueQueryResponse(base64Encode(valueSerde, result));
            } else {
                throw new ScalarValueNotFoundException(String.format("Key %s not found in store %s", deserializedKey.toString(), abstractQuery.getStoreName()));
            }
        });

    }


}
