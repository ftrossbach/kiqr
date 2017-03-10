package com.github.ftrossbach.kiqr.client.service.rest;

import com.github.ftrossbach.kiqr.client.service.GenericBlockingKiqrClient;
import com.github.ftrossbach.kiqr.client.service.SpecificBlockingKiqrClient;
import org.apache.kafka.common.serialization.Serde;
import java.util.Map;
import java.util.Optional;

/**
 * Created by ftr on 10/03/2017.
 */
public class SpecificBlockingRestKiqrClientImpl<K,V> implements SpecificBlockingKiqrClient<K,V>{


    private final String store;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    private final GenericBlockingKiqrClient genericClient;
    

    public SpecificBlockingRestKiqrClientImpl(String host, int port, String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.genericClient = initGenericService(host, port);
        this.store = store;
    }

    public SpecificBlockingRestKiqrClientImpl(GenericBlockingKiqrClient genericClient, String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde) {
        this.keyClass = keyClass;
        this.valueClass = valueClass;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.genericClient = genericClient;
        this.store = store;
    }

    protected GenericBlockingKiqrClient initGenericService(String host, int port){
       return new GenericBlockingRestKiqrClientImpl(host, port);
    }


    @Override
    public Optional<V> getScalarKeyValue( K key) {
        return genericClient.getScalarKeyValue(store, keyClass, key, valueClass, keySerde, valueSerde);
    }

    @Override
    public Map<K, V> getAllKeyValues() {
        return genericClient.getAllKeyValues(store, keyClass, valueClass, keySerde, valueSerde);
    }

    @Override
    public Map<K, V> getRangeKeyValues( K from, K to) {
        return genericClient.getRangeKeyValues(store, keyClass, valueClass, keySerde, valueSerde, from, to);
    }

    @Override
    public  Map<Long, V> getWindow( K key, long from, long to) {
        return genericClient.getWindow(store, keyClass, key, valueClass, keySerde, valueSerde, from, to);
    }
}
