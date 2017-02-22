package com.github.ftrossbach.kiqr.core.query.kv;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
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

            AllKeyValuesQuery query = (AllKeyValuesQuery) msg.body();

            Serde<Object> keySerde = getSerde(query.getKeySerde());
            Serde<Object> valueSerde = getSerde(query.getValueSerde());

            ReadOnlyKeyValueStore<Object, Object> kvStore = streams.store(query.getStoreName(), QueryableStoreTypes.keyValueStore());
            KeyValueIterator<Object, Object> result = kvStore.all();
            if (result.hasNext()) {
                Map<byte[], byte[]> results = new HashMap<>();
                while(result.hasNext()){
                    KeyValue<Object, Object> kvEntry = result.next();
                    results.put(serializeObject(keySerde, kvEntry.key), serializeObject(valueSerde, kvEntry.value));
                }
                msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.OK, results));
            } else {
                msg.reply(new MultiValuedKeyValueQueryResponse(QueryStatus.NOT_FOUND, Collections.emptyMap()));
            }

        });

    }


}
