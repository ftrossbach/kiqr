package com.github.ftrossbach.kiqr.core.query.kv;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.MultiValuedKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.QueryStatus;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.RangeKeyValueQuery;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import io.vertx.core.buffer.Buffer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ftr on 19/02/2017.
 */
public class RangeKeyValueQueryVerticle extends AbstractQueryVerticle {


    public RangeKeyValueQueryVerticle(String instanceId, KafkaStreams streams) {
        super(instanceId, streams);
    }



    @Override
    public void start() throws Exception {

        vertx.eventBus().consumer(Config.RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX + instanceId, msg -> {

            RangeKeyValueQuery query = (RangeKeyValueQuery) msg.body();

            Serde<Object> keySerde = getSerde(query.getKeySerde());
            Serde<Object> valueSerde = getSerde(query.getValueSerde());

            ReadOnlyKeyValueStore<Object, Object> kvStore = streams.store(query.getStoreName(), QueryableStoreTypes.keyValueStore());
            KeyValueIterator<Object, Object> result = kvStore.range(deserializeObject(keySerde, query.getFrom()), deserializeObject(keySerde, query.getTo()));
            if (result.hasNext()) {
                Map<String, String> results = new HashMap<>();
                while(result.hasNext()){
                    KeyValue<Object, Object> kvEntry = result.next();
                    results.put(base64Encode(keySerde, kvEntry.key),base64Encode(valueSerde, kvEntry.value));
                }
                msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.OK, results));
            } else {
                msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.NOT_FOUND, Collections.emptyMap()));
            }

        });

    }


}
