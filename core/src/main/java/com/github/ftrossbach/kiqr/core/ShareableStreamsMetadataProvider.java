package com.github.ftrossbach.kiqr.core;

import io.vertx.core.shareddata.Shareable;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.StreamsMetadata;
import java.util.Collection;

/**
 * Created by ftr on 15/03/2017.
 */
public class ShareableStreamsMetadataProvider implements Shareable {

    private final KafkaStreams streamsClient;

    public ShareableStreamsMetadataProvider(KafkaStreams streamsClient) {
        this.streamsClient = streamsClient;
    }


    public Collection<StreamsMetadata> allMetadata() {
        return streamsClient.allMetadata();
    }


    public Collection<StreamsMetadata> allMetadataForStore(final String storeName) {
        return streamsClient.allMetadataForStore(storeName);
    }

    public <K> StreamsMetadata metadataForKey(final String storeName,
                                              final K key,
                                              final Serializer<K> keySerializer) {

        return streamsClient.metadataForKey(storeName, key, keySerializer);
    }

    public <K> StreamsMetadata metadataForKey(final String storeName,
                                              final K key,
                                              final StreamPartitioner<? super K, ?> partitioner) {
        return streamsClient.metadataForKey(storeName, key, partitioner);
    }
}
