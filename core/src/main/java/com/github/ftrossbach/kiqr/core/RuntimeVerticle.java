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
import io.vertx.core.Future;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.Json;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

/**
 * Created by ftr on 18/02/2017.
 */
public class RuntimeVerticle extends AbstractVerticle{

    public static class Builder{

        private final KStreamBuilder builder;
        private final Properties properties;
        private Optional<HttpServerOptions> httpServerOptions;

        public Builder(KStreamBuilder builder) {
            this.builder = builder;
            properties = new Properties();
        }

        public Builder(KStreamBuilder builder, Properties properties) {
            this.builder = builder;
            this.properties = properties;
        }

        public Builder withApplicationId(String applicationId){
            properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
            return this;
        }
        public Builder withBootstrapServers(String servers){
            properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, servers);
            return this;
        }
        public Builder withBuffering(Integer buffer){
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, buffer.toString());
            return this;
        }

        public Builder withoutBuffering(){
            properties.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
            return this;
        }

        public Builder withKeySerde(Serde<?> serde) {
            properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, serde.getClass().getName());
            return this;
        }

        public Builder withKeySerde(Class<? extends Serde<?>> serdeClass) {
            properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, serdeClass.getName());
            return this;
        }

        public Builder withValueSerde(Serde<?> serde) {
            properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, serde.getClass().getName());
            return this;
        }

        public Builder withValueSerde(Class<? extends Serde<?>> serdeClass) {
            properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, serdeClass.getName());
            return this;
        }

        public Builder withHttpServer(HttpServerOptions options){
            this.httpServerOptions = Optional.ofNullable(options);
            return this;
        }

        public Builder withHttpServer(int port){
            this.httpServerOptions = Optional.ofNullable(new HttpServerOptions().setPort(port));
            return this;
        }


        public RuntimeVerticle build(){

            return httpServerOptions
                    .map(options -> new RuntimeVerticle(builder, properties, options))
                    .orElseGet(() -> new RuntimeVerticle(builder, properties));
        }



    }

    private KafkaStreams streams;
    private final KStreamBuilder builder;
    private final Properties props;
    private final Optional<HttpServerOptions> serverOptions;

    private RuntimeVerticle(KStreamBuilder builder, Properties props) {
       this(builder, props, null);
    }

    private RuntimeVerticle(KStreamBuilder builder, Properties props,HttpServerOptions serverOptions){
        this.builder = builder;
        this.props = props;
        this.serverOptions = Optional.ofNullable(serverOptions);
    }



    @Override
    public void start(Future<Void> startFuture) throws Exception {
        String instanceId = startStreams();

        registerCodecs();

        vertx.deployVerticle(new InstanceResolverVerticle(streams));
        vertx.deployVerticle(new KeyValueQueryVerticle(instanceId, streams));
        vertx.deployVerticle(new AllKeyValuesQueryVerticle(instanceId, streams));
        vertx.deployVerticle(new RangeKeyValueQueryVerticle(instanceId, streams));
        vertx.deployVerticle(new WindowedQueryVerticle(instanceId, streams));

        vertx.deployVerticle(new AllKeyValueQueryFacadeVerticle());
        vertx.deployVerticle(new KeyValueQueryFacadeVerticle());
        vertx.deployVerticle(new RangeKeyValueQueryFacadeVerticle());
        vertx.deployVerticle(new WindowedQueryFacadeVerticle());

        Json.mapper.registerModule(new Jdk8Module());

        if(serverOptions.isPresent()){
            vertx.deployVerticle(new HttpServer(serverOptions.get()), handler -> {
                if(handler.succeeded()){
                    startFuture.complete();
                } else {
                    startFuture.fail(handler.cause());
                }
            });
        } else {
            startFuture.complete();
        }

    }

    private String startStreams() {
        String instanceId = UUID.randomUUID().toString();
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, instanceId + ":124");

        streams = new KafkaStreams(builder, props);

        streams.start();
        return instanceId;
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

    private <T> void registerCodec(Class<T> clazz){
        vertx.eventBus().registerDefaultCodec(clazz, new KiqrCodec<>(clazz));
    }

    @Override
    public void stop() throws Exception {
        streams.close();
    }
}
