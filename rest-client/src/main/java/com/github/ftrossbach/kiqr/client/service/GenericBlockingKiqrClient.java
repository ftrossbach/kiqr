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
package com.github.ftrossbach.kiqr.client.service;

import org.apache.kafka.common.serialization.Serde;

import java.util.Map;
import java.util.Optional;

/**
 * Created by ftr on 01/03/2017.
 */
public interface GenericBlockingKiqrClient {

    <K,V > Optional<V> getScalarKeyValue(String store, Class<K> keyClass, K key, Class<V> valueClass, Serde<K> keySerde,Serde<V> valueSerde);

    <K,V> Map<K,V> getAllKeyValues(String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde,Serde<V> valueSerde);

    <K,V> Map<K,V> getRangeKeyValues(String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde,Serde<V> valueSerde, K from, K to);

    <K,V> Map<Long,V> getWindow(String store, Class<K> keyClass, K key, Class<V> valueClass,Serde<K> keySerde,Serde<V> valueSerde, long from, long to);

}
