package com.github.ftrossbach.kiqr.core.query.session;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.*;
import java.util.Collections;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created by ftr on 16/03/2017.
 */
public class SessionWindowQueryVerticle extends AbstractQueryVerticle {
    public SessionWindowQueryVerticle(String instanceId, KafkaStreams streams) {
        super(instanceId, streams);
    }

    @Override
    public void start() throws Exception {

        execute(Config.SESSION_QUERY_ADDRESS_PREFIX, (abstractQuery, keySerde, valueSerde) -> {

            KeyBasedQuery query = (KeyBasedQuery) abstractQuery;
            ReadOnlySessionStore<Object, Object> store = streams.store(query.getStoreName(), QueryableStoreTypes.sessionStore());
            try (KeyValueIterator<Windowed<Object>, Object> result = store.fetch(deserializeObject(keySerde, query.getKey()))) {

                if (result.hasNext()) {
                    SortedMap<Window, String> results = new TreeMap<>();
                    while (result.hasNext()) {


                        KeyValue<Windowed<Object>, Object> windowedEntry = result.next();
                        results.put(new Window(windowedEntry.key.window().start(), windowedEntry.key.window().end()), base64Encode(valueSerde, windowedEntry.value));
                    }
                    return new SessionQueryResponse(results);
                } else {
                    return new SessionQueryResponse(Collections.emptySortedMap());
                }
            }
        });


    }
}