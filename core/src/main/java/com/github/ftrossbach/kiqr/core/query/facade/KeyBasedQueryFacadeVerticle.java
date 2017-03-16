package com.github.ftrossbach.kiqr.core.query.facade;

import com.github.ftrossbach.kiqr.commons.config.Config;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import com.github.ftrossbach.kiqr.core.ShareableStreamsMetadataProvider;
import com.github.ftrossbach.kiqr.core.query.AbstractKiqrVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.ReplyException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StreamsMetadata;

/**
 * Created by ftr on 15/03/2017.
 */
public class KeyBasedQueryFacadeVerticle<REQ extends AbstractQuery & HasKey, RES> extends AbstractKiqrVerticle{

    private final String listeningAddress;
    private final String queryAddressPrefix;

    public KeyBasedQueryFacadeVerticle(String listeningAddress, String queryAddressPrefix) {
        this.listeningAddress = listeningAddress;
        this.queryAddressPrefix = queryAddressPrefix;
    }

    @Override
    public void start(Future<Void> startFuture){
        vertx.eventBus().consumer(listeningAddress, msg -> {
            REQ query = (REQ) msg.body();

            ShareableStreamsMetadataProvider metadata = (ShareableStreamsMetadataProvider) vertx.sharedData().getLocalMap("metadata").get("metadata");
            Serde<Object> serde = getSerde(query.getKeySerde());

            Object deserializedKey = serde.deserializer().deserialize("?", ((HasKey)query).getKey());

            StreamsMetadata streamsMetadata = metadata.metadataForKey(query.getStoreName(), deserializedKey, serde.serializer());

            if(streamsMetadata == null ) {
                msg.fail(404, "Store not found: " + query.getStoreName());
            } else if("unavailable".equals(streamsMetadata.host())){

                msg.fail(503, "Streaming application currently unavailable");
            } else {
                vertx.eventBus().send(queryAddressPrefix + streamsMetadata.host(), query, rep -> {

                    if(rep.succeeded()){
                        RES queryResponse = (RES) rep.result().body();

                        msg.reply(queryResponse);
                    }
                    else {
                        if(rep.cause() instanceof ReplyException){
                            ReplyException cause = (ReplyException) rep.cause();
                            msg.fail(cause.failureCode(), cause.getMessage());
                        } else {
                            msg.fail(500, rep.cause().getMessage());
                        }
                    }

                });
            }


        });

        startFuture.complete();
    }
}
