package com.github.ftrossbach.kiqr.client.service;

import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.Window;
import org.apache.kafka.common.serialization.Serde;
import java.util.Map;
import java.util.Optional;

/**
 * Created by ftr on 10/03/2017.
 */
public interface SpecificBlockingKiqrClient<K,V> {

    Optional<V> getScalarKeyValue(K key);

    Map<K,V> getAllKeyValues();

    Map<K,V> getRangeKeyValues(K from, K to);

    Map<Long,V> getWindow(K key, long from, long to);

    Optional<Long> count(String store);

    Map<Window, V> getSession(K key);

}
