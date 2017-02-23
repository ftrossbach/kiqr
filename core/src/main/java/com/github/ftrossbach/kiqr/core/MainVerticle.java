package com.github.ftrossbach.kiqr.core;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

import java.util.Properties;

/**
 * Created by ftr on 19/02/2017.
 */
public class MainVerticle extends AbstractVerticle {
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "kiqr");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        KStreamBuilder builder = new KStreamBuilder();
        KTable<String, Long> table = builder.table(Serdes.String(), Serdes.Long(), "visits", "visitStore");
        KTable<Windowed<String>, Long> windowedCount = table.toStream().groupByKey().count(TimeWindows.of(60), "visitCount");


        ScalarKeyValueQuery kvQuery = new ScalarKeyValueQuery("visitStore", "org.apache.kafka.common.serialization.Serdes$StringSerde", Serdes.String().serializer().serialize("?", "127.0.0.1"), "org.apache.kafka.common.serialization.Serdes$LongSerde" );
         WindowedQuery windowQuery = new WindowedQuery("visitCount", "org.apache.kafka.common.serialization.Serdes$StringSerde",Serdes.String().serializer().serialize("", "127.0.0.1"),"org.apache.kafka.common.serialization.Serdes$LongSerde", System.currentTimeMillis() - 1000 * 60 * 60, System.currentTimeMillis());
        RangeKeyValueQuery rangeQuery = new RangeKeyValueQuery("visitStore", "org.apache.kafka.common.serialization.Serdes$StringSerde", "org.apache.kafka.common.serialization.Serdes$LongSerde",Serdes.String().serializer().serialize("?", "127.0.0.1"), Serdes.String().serializer().serialize("?", "127.0.0.3") );
        AllKeyValuesQuery allKvQuery = new AllKeyValuesQuery("visitStore", "org.apache.kafka.common.serialization.Serdes$StringSerde", "org.apache.kafka.common.serialization.Serdes$LongSerde");

        vertx.setPeriodic(10000, id -> {

            /*vertx.eventBus().send(Config.KEY_VALUE_QUERY_FACADE_ADDRESS, kvQuery, rep -> {

                if(rep.succeeded()){
                    System.out.println(rep.result().body());
                } else {
                    rep.cause().printStackTrace();
                }
            });*/

            /*vertx.eventBus().send(Config.WINDOWED_QUERY_FACADE_ADDRESS, windowQuery, rep -> {

                if(rep.succeeded()){
                    System.out.println(rep.result().body());
                } else {
                    rep.cause().printStackTrace();
                }
            });*/

            /*vertx.eventBus().send(Config.RANGE_KEY_VALUE_QUERY_FACADE_ADDRESS, rangeQuery, rep -> {

                if(rep.succeeded()){
                    System.out.println(rep.result().body());
                } else {
                    rep.cause().printStackTrace();
                }
            });*/
            vertx.eventBus().send(Config.ALL_KEY_VALUE_QUERY_FACADE_ADDRESS, allKvQuery, rep -> {

                if(rep.succeeded()){
                    System.out.println(rep.result().body());
                } else {
                    rep.cause().printStackTrace();
                }
            });


        });

        vertx.deployVerticle(new RuntimeVerticle(builder, props), res -> {
            if (res.succeeded()) {
                startFuture.complete();
            } else {
                startFuture.fail(res.cause());
            }
        });
    }
}

