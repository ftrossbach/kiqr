package com.github.ftrossbach.kiqr.core;

import com.github.ftrossbach.kiqr.commons.config.querymodel.codec.*;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.query.InstanceResolverVerticle;
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
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.util.Properties;
import java.util.UUID;

/**
 * Created by ftr on 18/02/2017.
 */
public class RuntimeVerticle extends AbstractVerticle{

    final Logger logger = LoggerFactory.getLogger(getClass());

    private  KafkaStreams streams;
    private KStreamBuilder builder;
    private Properties props;


    public RuntimeVerticle(KStreamBuilder builder, Properties props) {
        this.builder = builder;
        this.props = props;
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

        vertx.deployVerticle(new HttpServer());

        startFuture.complete();

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
