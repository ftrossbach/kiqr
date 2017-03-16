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
package com.github.ftrossbach.kiqr.core;

import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import com.github.ftrossbach.kiqr.core.query.facade.KeyBasedQueryFacadeVerticle;
import com.github.ftrossbach.kiqr.core.query.facade.ScatterGatherQueryFacadeVerticle;
import com.github.ftrossbach.kiqr.core.query.kv.AllKeyValuesQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.kv.KeyValueCountVerticle;
import com.github.ftrossbach.kiqr.core.query.kv.KeyValueQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.kv.RangeKeyValueQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.session.SessionWindowQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.windowed.WindowedQueryVerticle;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BinaryOperator;
import java.util.function.Supplier;
import java.util.stream.Stream;


import static java.util.stream.Collectors.toList;

/**
 * Created by ftr on 18/02/2017.
 */
public class RuntimeVerticle extends AbstractVerticle {

    private static final Logger LOG = LoggerFactory.getLogger(RuntimeVerticle.class);

    public static class Builder<T extends Builder> {

        private final KStreamBuilder builder;
        private final Properties properties;
        private KafkaStreams.StateListener listener;

        public static Builder<Builder> builder(KStreamBuilder builder){
            return new Builder<>(builder);
        }

        public static Builder<Builder> builder(KStreamBuilder builder, Properties props){
            return new Builder<>(builder, props);
        }

        protected Builder(KStreamBuilder builder) {
            this.builder = builder;
            properties = new Properties();
        }

        protected Builder(KStreamBuilder builder, Properties properties) {
            this.builder = builder;
            this.properties = properties;
        }

        public T withApplicationId(String applicationId) {
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            return (T) this;
        }

        public T withBootstrapServers(String servers) {
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return (T) this;
        }

        public T withBuffering(Integer buffer) {
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, buffer);
            return (T) this;
        }

        public T withoutBuffering() {
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            return (T) this;
        }

        public T withKeySerde(Serde<?> serde) {
            properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, serde.getClass().getName());
            return (T) this;
        }

        public T withKeySerde(Class<? extends Serde> serdeClass) {
            properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, serdeClass.getName());
            return (T) this;
        }

        public T withValueSerde(Serde<?> serde) {
            properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, serde.getClass().getName());
            return (T) this;
        }

        public T withValueSerde(Class<? extends Serde> serdeClass) {
            properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, serdeClass.getName());
            return (T) this;
        }

        public T withStateListener(KafkaStreams.StateListener listener){
            this.listener = listener;
            return (T) this;
        }

        public AbstractVerticle build() {

            return new RuntimeVerticle(builder, properties, listener);
        }


    }

    private final KStreamBuilder builder;
    protected final Properties props;
    private final KafkaStreams.StateListener listener;
    private KafkaStreams streams;


    protected RuntimeVerticle(KStreamBuilder builder, Properties props, KafkaStreams.StateListener listener) {
        this.builder = builder;
        this.props = props;
        this.listener = listener;
    }




    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Json.mapper.registerModule(new Jdk8Module());
        registerCodecs();
        String instanceId = UUID.randomUUID().toString();
        vertx.<KafkaStreams>executeBlocking(future -> {

            LOG.info("Starting KafkaStreams with application.server set to " + instanceId);
            //starting streams can take a while, therefore we do it asynchronously
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, instanceId + ":124");
            KafkaStreams streams = createAndStartStream();
            vertx.sharedData().getLocalMap("metadata").put("metadata", new ShareableStreamsMetadataProvider(streams));
            future.complete(streams);

        }, res -> {

            if (res.succeeded()) {
                LOG.info("Started KafkaStreams, deploying query verticles");

                Supplier<MultiValuedKeyValueQueryResponse> multiValuedIdentity = () -> new MultiValuedKeyValueQueryResponse();
                BinaryOperator<MultiValuedKeyValueQueryResponse> multiValuedReducer = (a, b) -> a.merge(b);
                KafkaStreams stream = res.result();
                Future deployFuture = deployVerticles(new KeyValueQueryVerticle(instanceId, stream),
                        new AllKeyValuesQueryVerticle(instanceId, stream),
                        new RangeKeyValueQueryVerticle(instanceId, stream),
                        new WindowedQueryVerticle(instanceId, stream),
                        new KeyValueCountVerticle(instanceId, stream),
                        new SessionWindowQueryVerticle(instanceId, stream),
                        new KeyBasedQueryFacadeVerticle<KeyBasedQuery, ScalarKeyValueQueryResponse>(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, Config.KEY_VALUE_QUERY_ADDRESS_PREFIX),
                        new ScatterGatherQueryFacadeVerticle<MultiValuedKeyValueQueryResponse>(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, Config.ALL_KEY_VALUE_QUERY_ADDRESS_PREFIX, multiValuedIdentity, multiValuedReducer),
                        new ScatterGatherQueryFacadeVerticle<MultiValuedKeyValueQueryResponse>(Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS, Config.RANGE_KEY_VALUE_QUERY_ADDRESS_PREFIX, multiValuedIdentity, multiValuedReducer),
                        new KeyBasedQueryFacadeVerticle<WindowedQuery, WindowedQueryResponse>(Config.WINDOWED_QUERY_FACADE_ADDRESS, Config.WINDOWED_QUERY_ADDRESS_PREFIX),
                        new ScatterGatherQueryFacadeVerticle<Long>(Config.COUNT_KEY_VALUE_QUERY_FACADE_ADDRESS, Config.COUNT_KEY_VALUE_QUERY_ADDRESS_PREFIX, () -> 0L, (a,b) -> a+b),
                        new KeyBasedQueryFacadeVerticle<KeyBasedQuery, SessionQueryResponse>(Config.SESSION_QUERY_FACADE_ADDRESS, Config.SESSION_QUERY_ADDRESS_PREFIX)
                );



                deployFuture.setHandler(handler -> {

                    AsyncResult ar = (AsyncResult) handler;
                    if (ar.succeeded()) {
                        LOG.info("Deployed query verticles");
                        startFuture.complete();
                    } else {
                        LOG.error("Deploying query verticles failed", ar);
                        startFuture.fail(ar.cause());
                    }
                });

            } else {
                LOG.error("Starting KafkaStreams failed", res.cause());
                startFuture.fail(res.cause());
            }


        });


    }

    protected  KafkaStreams createAndStartStream(){
        streams = new KafkaStreams(builder, props);



        streams.setStateListener(((newState, oldState) -> {

            vertx.eventBus().publish(Config.CLUSTER_STATE_BROADCAST_ADDRESS, newState.toString());
            LOG.info("State change in KafkaStreams recorded: oldstate=" + oldState +  ", newstate=" + newState);
            if(listener != null) listener.onChange(newState, oldState);
        }));

        streams.start();
        return streams;
    }

    private Future deployVerticles(AbstractVerticle... verticles) {


        Stream<AbstractVerticle> stream = Arrays.stream(verticles);

        List<Future> futures = stream
                .map(verticle -> {

                    Future<String> future = Future.<String>future();

                    vertx.deployVerticle(verticle, future.completer());

                    return future;
                })
                .map(future -> (Future) future)
                .collect(toList());

        return CompositeFuture.all(futures);

    }


    private void registerCodecs() {
        registerCodec(KeyBasedQuery.class);
        registerCodec(WindowedQuery.class);
        registerCodec(ScalarKeyValueQueryResponse.class);
        registerCodec(MultiValuedKeyValueQueryResponse.class);
        registerCodec(WindowedQueryResponse.class);
        registerCodec(StoreWideQuery.class);
        registerCodec(RangeKeyValueQuery.class);
        registerCodec(AllInstancesResponse.class);
        registerCodec(KeyValueStoreCountQuery.class);
        registerCodec(SessionQueryResponse.class);

    }

    private <T> void registerCodec(Class<T> clazz) {
        vertx.eventBus().registerDefaultCodec(clazz, new KiqrCodec<>(clazz));
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        vertx.<Void>executeBlocking(future -> {

            LOG.info("Shutting down KafkaStreams");
           streams.close();
           future.complete();

        }, msg -> {

        });
    }
}
