package com.github.ftrossbach.kiqr.core.query.windowed;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import io.vertx.core.buffer.Buffer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
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

            WindowedQuery query = (WindowedQuery) msg.body();

            Serde<Object> keySerde = getSerde(query.getKeySerde());
            Serde<Object> valueSerde = getSerde(query.getValueSerde());

            ReadOnlyWindowStore<Object, Object> windowStore = streams.store(query.getStoreName(), QueryableStoreTypes.windowStore());
            WindowStoreIterator<Object> result = windowStore.fetch(deserializeObject(keySerde, query.getKey()), query.getFrom(), query.getTo());
            if (result.hasNext()) {
                SortedMap<Long, Buffer> results = new TreeMap<>();
                while (result.hasNext()) {
                    KeyValue<Long, Object> windowedEntry = result.next();
                    results.put(windowedEntry.key, Buffer.buffer(serializeObject(valueSerde, windowedEntry.value)));
                }
                msg.reply(new WindowedQueryResponse(QueryStatus.OK, results));
            } else {
                msg.reply(new WindowedQueryResponse(QueryStatus.NOT_FOUND, Collections.emptySortedMap()));
            }

        });

    }


}
