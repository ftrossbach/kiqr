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
package com.github.ftrossbach.kiqr.core.query.windowed;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
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

        execute(Config.WINDOWED_QUERY_ADDRESS_PREFIX, (abstractQuery, keySerde, valueSerde) -> {

            WindowedQuery query = (WindowedQuery) abstractQuery;
            ReadOnlyWindowStore<Object, Object> windowStore = streams.store(query.getStoreName(), QueryableStoreTypes.windowStore());

            try (WindowStoreIterator<Object> result = windowStore.fetch(deserializeObject(keySerde, query.getKey()), query.getFrom(), query.getTo())) {

                if (result.hasNext()) {
                    SortedMap<Long, String> results = new TreeMap<>();
                    while (result.hasNext()) {
                        KeyValue<Long, Object> windowedEntry = result.next();
                        results.put(windowedEntry.key, base64Encode(valueSerde, windowedEntry.value));
                    }
                    return new WindowedQueryResponse(results);
                } else {
                    return new WindowedQueryResponse(Collections.emptySortedMap());
                }
            }
        });


    }


}
