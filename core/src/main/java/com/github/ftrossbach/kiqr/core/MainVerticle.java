package com.github.ftrossbach.kiqr.core;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.InstanceResolverResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQuery;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;

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

        KStreamBuilder builder = new KStreamBuilder();
        builder.table(Serdes.String(), Serdes.Long(), "visits", "visitStore");


        vertx.setPeriodic(10000, id -> {

            InstanceResolverQuery query = new InstanceResolverQuery("visitStore", "org.apache.kafka.common.serialization.Serdes$StringSerde",Serdes.String().serializer().serialize("?", "127.0.0.1") );
            vertx.eventBus().send(Config.INSTANCE_RESOLVER_ADDRESS, query, reply -> {


                if(reply.succeeded()){

                    InstanceResolverResponse response = (InstanceResolverResponse) reply.result().body();

                    ScalarKeyValueQuery kvQuery = new ScalarKeyValueQuery("visitStore", "org.apache.kafka.common.serialization.Serdes$StringSerde", Serdes.String().serializer().serialize("?", "127.0.0.1"), "org.apache.kafka.common.serialization.Serdes$LongSerde" );

                    vertx.eventBus().send(Config.KEY_VALUE_QUERY_ADDRESS_PREFIX + response.getInstanceId().get(), kvQuery, rep -> {

                        ScalarKeyValueQueryResponse queryResponse = (ScalarKeyValueQueryResponse) rep.result().body();


                        System.out.println(Serdes.Long().deserializer().deserialize("", queryResponse.getValue()));
                    });
                } else {
                    reply.cause().printStackTrace();
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

