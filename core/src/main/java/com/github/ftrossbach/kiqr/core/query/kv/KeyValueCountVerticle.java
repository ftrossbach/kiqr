package com.github.ftrossbach.kiqr.core.query.kv;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.AbstractQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.KeyValueStoreCountQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.core.query.AbstractQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.exceptions.ScalarValueNotFoundException;
import com.github.ftrossbach.kiqr.core.query.exceptions.SerdeNotFoundException;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

/**
 * Created by ftr on 15/03/2017.
 */
public class KeyValueCountVerticle extends AbstractQueryVerticle{
    private static final Logger LOG = LoggerFactory.getLogger(KeyValueCountVerticle.class);


    public KeyValueCountVerticle(String instanceId, KafkaStreams streams) {
        super(instanceId, streams);
    }

    @Override
    public void start() throws Exception {

        vertx.eventBus().consumer(Config.COUNT_KEY_VALUE_QUERY_ADDRESS_PREFIX + instanceId, msg -> {
            KeyValueStoreCountQuery query = (KeyValueStoreCountQuery) msg.body();
            try {
                ReadOnlyKeyValueStore<Object, Object> kvStore = streams.store(query.getStoreName(), QueryableStoreTypes.keyValueStore());
                long count = kvStore.approximateNumEntries();
                msg.reply(count);
            } catch (InvalidStateStoreException e) {
                LOG.error(String.format("Store %s not queriable. Could be due to wrong store type or rebalancing", query.getStoreName()));
                msg.fail(500, String.format("Store not available due to rebalancing or wrong store type"));
            } catch (RuntimeException e) {
                LOG.error("Unexpected exception", e);
                msg.fail(500, e.getMessage());
            }
        });

    }
}
