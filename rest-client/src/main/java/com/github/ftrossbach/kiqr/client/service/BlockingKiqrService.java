package com.github.ftrossbach.kiqr.client.service;

import org.apache.kafka.common.serialization.Serde;

import java.util.Map;
import java.util.Optional;

/**
 * Created by ftr on 01/03/2017.
 */
public interface BlockingKiqrService {

    <K,V > Optional<V> getScalarKeyValue(String store, Class<K> keyClass, K key, Class<V> valueClass, Serde<K> keySerde,Serde<V> valueSerde);

    <K,V> Map<K,V> getAllKeyValues(String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde,Serde<V> valueSerde);

    <K,V> Map<K,V> getRangeKeyValues(String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde,Serde<V> valueSerde, K from, K to);

    <K,V> Map<Long,V> getWindow(String store, Class<K> keyClass, K key, Class<V> valueClass,Serde<K> keySerde,Serde<V> valueSerde, long from, long to);

}
