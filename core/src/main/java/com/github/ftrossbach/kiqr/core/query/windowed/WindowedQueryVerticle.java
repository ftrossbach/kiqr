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
package com.github.ftrossbach.kiqr.core.query.windowed;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.AbstractKiqrVerticle;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.exceptions.SerdeNotFoundException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.*;

import java.util.*;

/**
 * Created by ftr on 19/02/2017.
 */
public class WindowedQueryVerticle extends AbstractQueryVerticle {


    public WindowedQueryVerticle(String instanceId, KafkaStreams streams) {
        super(instanceId, streams);
    }


    @Override
    public void start() throws Exception {

        vertx.eventBus().consumer(Config.WINDOWED_QUERY_ADDRESS_PREFIX + instanceId, msg -> {

            try {
                WindowedQuery query = (WindowedQuery) msg.body();

                Serde<Object> keySerde = getSerde(query.getKeySerde());
                Serde<Object> valueSerde = getSerde(query.getValueSerde());

                ReadOnlyWindowStore<Object, Object> windowStore = streams.store(query.getStoreName(), QueryableStoreTypes.windowStore());
                try (WindowStoreIterator<Object> result = windowStore.fetch(deserializeObject(keySerde, query.getKey()), query.getFrom(), query.getTo())) {

                    if (result.hasNext()) {
                        SortedMap<Long, String> results = new TreeMap<>();
                        while (result.hasNext()) {
                            KeyValue<Long, Object> windowedEntry = result.next();
                            results.put(windowedEntry.key, base64Encode(valueSerde, windowedEntry.value));
                        }
                        msg.reply(new WindowedQueryResponse(QueryStatus.OK, results));
                    } else {
                        msg.reply(new WindowedQueryResponse(QueryStatus.NOT_FOUND, Collections.emptySortedMap()));
                    }
                }
            } catch (SerdeNotFoundException e) {
                msg.fail(400, e.getMessage());
            } catch (InvalidStateStoreException e) {
                msg.fail(409, String.format("Store not available due to rebalancing"));
            } catch (RuntimeException e) {
                msg.fail(500, e.getMessage());
            }

        });

    }


}
