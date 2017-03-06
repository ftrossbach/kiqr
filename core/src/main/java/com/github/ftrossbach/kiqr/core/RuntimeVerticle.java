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
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.InstanceResolverVerticle;
import com.github.ftrossbach.kiqr.core.query.KiqrCodec;
import com.github.ftrossbach.kiqr.core.query.facade.AllKeyValueQueryFacadeVerticle;
import com.github.ftrossbach.kiqr.core.query.facade.KeyValueQueryFacadeVerticle;
import com.github.ftrossbach.kiqr.core.query.facade.RangeKeyValueQueryFacadeVerticle;
import com.github.ftrossbach.kiqr.core.query.facade.WindowedQueryFacadeVerticle;
import com.github.ftrossbach.kiqr.core.query.kv.AllKeyValuesQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.kv.KeyValueQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.kv.RangeKeyValueQueryVerticle;
import com.github.ftrossbach.kiqr.core.query.windowed.WindowedQueryVerticle;
import com.github.ftrossbach.kiqr.rest.server.HttpServer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by ftr on 18/02/2017.
 */
public class RuntimeVerticle extends AbstractVerticle {

    public static class Builder {

        private final KStreamBuilder builder;
        private final Properties properties;
        private Optional<HttpServerOptions> httpServerOptions = Optional.empty();

        public Builder(KStreamBuilder builder) {
            this.builder = builder;
            properties = new Properties();
        }

        public Builder(KStreamBuilder builder, Properties properties) {
            this.builder = builder;
            this.properties = properties;
        }

        public Builder withApplicationId(String applicationId) {
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            return this;
        }

        public Builder withBootstrapServers(String servers) {
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return this;
        }

        public Builder withBuffering(Integer buffer) {
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, buffer);
            return this;
        }

        public Builder withoutBuffering() {
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
            return this;
        }

        public Builder withKeySerde(Serde<?> serde) {
            properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, serde.getClass().getName());
            return this;
        }

        public Builder withKeySerde(Class<? extends Serde> serdeClass) {
            properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, serdeClass.getName());
            return this;
        }

        public Builder withValueSerde(Serde<?> serde) {
            properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, serde.getClass().getName());
            return this;
        }

        public Builder withValueSerde(Class<? extends Serde> serdeClass) {
            properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, serdeClass.getName());
            return this;
        }

        public Builder withHttpServer(HttpServerOptions options) {
            this.httpServerOptions = Optional.ofNullable(options);
            return this;
        }

        public Builder withHttpServer(int port) {
            this.httpServerOptions = Optional.ofNullable(new HttpServerOptions().setPort(port));
            return this;
        }


        public RuntimeVerticle build() {

            return httpServerOptions
                    .map(options -> new RuntimeVerticle(builder, properties, options))
                    .orElseGet(() -> new RuntimeVerticle(builder, properties));
        }


    }

    private final KStreamBuilder builder;
    protected final Properties props;
    protected final Optional<HttpServerOptions> serverOptions;

    protected RuntimeVerticle(KStreamBuilder builder, Properties props) {
        this(builder, props, null);
    }

    protected RuntimeVerticle(KStreamBuilder builder, Properties props, HttpServerOptions serverOptions) {
        this.builder = builder;
        this.props = props;
        this.serverOptions = Optional.ofNullable(serverOptions);
    }


    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Json.mapper.registerModule(new Jdk8Module());
        registerCodecs();
        String instanceId = UUID.randomUUID().toString();
        vertx.<KafkaStreams>executeBlocking(future -> {

            //starting streams can take a while, therefore we do it asynchronously
            props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, instanceId + ":124");
            KafkaStreams streams = createAndStartStream();
            future.complete(streams);

        }, res -> {

            if (res.succeeded()) {

                Future deployFuture = deployVerticles(new InstanceResolverVerticle(res.result()), new KeyValueQueryVerticle(instanceId, res.result()),
                        new AllKeyValuesQueryVerticle(instanceId, res.result()), new RangeKeyValueQueryVerticle(instanceId, res.result()),
                        new WindowedQueryVerticle(instanceId, res.result()), new AllKeyValueQueryFacadeVerticle(),
                        new KeyValueQueryFacadeVerticle(), new RangeKeyValueQueryFacadeVerticle(), new WindowedQueryFacadeVerticle());


                if (serverOptions.isPresent()) {
                    deployFuture = CompositeFuture.all(deployFuture, deployVerticles(new HttpServer(serverOptions.get())));
                }


                deployFuture.setHandler(handler -> {
                    AsyncResult ar = (AsyncResult) handler;
                    if (ar.succeeded()) {
                        startFuture.complete();
                    } else {
                        startFuture.fail(ar.cause());
                    }
                });


            } else {
                startFuture.fail(res.cause());
            }


        });


    }

    protected  KafkaStreams createAndStartStream(){
        KafkaStreams streams = new KafkaStreams(builder, props);
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
                .collect(Collectors.toList());

        return CompositeFuture.all(futures);

    }


    private void registerCodecs() {
        registerCodec(InstanceResolverQuery.class);
        registerCodec(ScalarKeyValueQuery.class);
        registerCodec(WindowedQuery.class);
        registerCodec(ScalarKeyValueQueryResponse.class);
        registerCodec(MultiValuedKeyValueQueryResponse.class);
        registerCodec(WindowedQueryResponse.class);
        registerCodec(InstanceResolverResponse.class);
        registerCodec(AllKeyValuesQuery.class);
        registerCodec(RangeKeyValueQuery.class);
        registerCodec(AllInstancesResponse.class);

    }

    private <T> void registerCodec(Class<T> clazz) {
        vertx.eventBus().registerDefaultCodec(clazz, new KiqrCodec<>(clazz));
    }


}
