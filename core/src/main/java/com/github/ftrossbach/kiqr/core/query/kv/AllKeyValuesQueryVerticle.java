/**
 * Copyright © 2017 Florian Troßbach (trossbach@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ftrossbach.kiqr.core.query.kv;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.AbstractKiqrVerticle;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.exceptions.SerdeNotFoundException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.*;

/**
 * Created by ftr on 19/02/2017.
 */
public class AllKeyValuesQueryVerticle extends AbstractQueryVerticle {


    public AllKeyValuesQueryVerticle(String instanceId, KafkaStreams streams) {
        super(instanceId, streams);
    }


    @Override
    public void start() throws Exception {

        vertx.eventBus().consumer(Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX + instanceId, msg -> {

            try {
                AllKeyValuesQuery query = (AllKeyValuesQuery) msg.body();

                Serde<Object> keySerde = getSerde(query.getKeySerde());
                Serde<Object> valueSerde = getSerde(query.getValueSerde());

                ReadOnlyKeyValueStore<Object, Object> kvStore = streams.store(query.getStoreName(), QueryableStoreTypes.keyValueStore());
                try (KeyValueIterator<Object, Object> result = kvStore.all()) {
                    if (result.hasNext()) {
                        Map<String, String> results = new HashMap<>();
                        while (result.hasNext()) {
                            KeyValue<Object, Object> kvEntry = result.next();

                            results.put(base64Encode(keySerde, kvEntry.key), base64Encode(valueSerde, kvEntry.value));
                        }
                        msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.OK, results));
                    } else {
                        msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.NOT_FOUND, Collections.emptyMap()));
                    }
                }
            } catch (SerdeNotFoundException e) {
                msg.fail(400, e.getMessage());
            } catch (InvalidStateStoreException e) {
                msg.fail(409, String.format("Store not available due to rebalancing"));
            }
            catch (RuntimeException e) {
                msg.fail(500, e.getMessage());
            }
        });

    }


}
