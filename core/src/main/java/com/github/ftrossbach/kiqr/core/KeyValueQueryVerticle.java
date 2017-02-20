package com.github.ftrossbach.kiqr.core;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.QueryStatus;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
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
    protected String getQueryAddressPrefix() {
        return Config.KEY_VALUE_QUERY_ADDRESS_PREFIX;
    }

    @Override
    public void start() throws Exception {

        vertx.eventBus().consumer(getQueryAddressPrefix() + instanceId, msg -> {

            ScalarKeyValueQuery query = (ScalarKeyValueQuery) msg.body();

            Serde<Object> keySerde = getSerde(query.getKeySerde());
            Serde<Object> valueSerde = getSerde(query.getValueSerde());

            Object deserializedKey = deserializeObject(keySerde, query.getKey());
            ReadOnlyKeyValueStore<Object, Object> kvStore = streams.store(query.getStoreName(), QueryableStoreTypes.keyValueStore());
            Object result = kvStore.get(deserializedKey);
            if (result != null) {
                msg.reply(new ScalarKeyValueQueryResponse(QueryStatus.OK, serializeObject(valueSerde, result)));
            } else {
                msg.reply(new ScalarKeyValueQueryResponse(QueryStatus.NOT_FOUND, null));
            }

        });

    }


}
