/**
 * Copyright © 2017 Florian Troßbach (trossbach@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.ftrossbach.kiqr.client.service.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ftrossbach.kiqr.client.service.GenericBlockingKiqrClient;
import com.github.ftrossbach.kiqr.client.service.ConnectionException;
import com.github.ftrossbach.kiqr.client.service.QueryExecutionException;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.serialization.Serde;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Created by ftr on 01/03/2017.
 */
public class GenericBlockingRestKiqrClientImpl implements GenericBlockingKiqrClient {

    private final String host;
    private final int port;
    private final ObjectMapper mapper;

    public GenericBlockingRestKiqrClientImpl(String host, int port) {
        this.host = host;
        this.port = port;
        mapper = new ObjectMapper();
    }

    @Override
    public <K, V> Optional<V> getScalarKeyValue(String store, Class<K> keyClass, K key, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde) {


        return execute(() -> getUriBuilder()
                .setPath(String.format("/api/v1/kv/%s/values/%s", store, Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", key))))
                .addParameter("keySerde", keySerde.getClass().getName())
                .addParameter("valueSerde", valueSerde.getClass().getName())
                .build(), bytes -> {
            ScalarKeyValueQueryResponse resp = mapper.readValue(bytes, ScalarKeyValueQueryResponse.class);

            return Optional.of(deserialize(valueClass, valueSerde, resp.getValue()));

        }, () -> Optional.<V>empty());


    }

    @Override
    public <K, V> Map<K, V> getAllKeyValues(String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde) {

        return execute(() -> getUriBuilder()
                .setPath(String.format("/api/v1/kv/%s", store))
                .addParameter("keySerde", keySerde.getClass().getName())
                .addParameter("valueSerde", valueSerde.getClass().getName())
                .build(), bytes -> {


            MultiValuedKeyValueQueryResponse resp = mapper.readValue(bytes, MultiValuedKeyValueQueryResponse.class);


            return resp.getResults().entrySet().stream()
                    .map(entry -> {
                        return new Pair<K, V>(deserialize(keyClass, keySerde, entry.getKey()),
                                deserialize(valueClass, valueSerde, entry.getValue()));
                    }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue()));

        }, () -> Collections.emptyMap());


    }

    @Override
    public <K, V> Map<K, V> getRangeKeyValues(String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde, K from, K to) {


        return execute(() -> getUriBuilder()
                .setPath(String.format("/api/v1/kv/%s", store))
                .addParameter("keySerde", keySerde.getClass().getName())
                .addParameter("valueSerde", valueSerde.getClass().getName())
                .addParameter("from", Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", from)))
                .addParameter("to", Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", to)))
                .build(), bytes -> {
            MultiValuedKeyValueQueryResponse resp = mapper.readValue(bytes, MultiValuedKeyValueQueryResponse.class);


            return resp.getResults().entrySet().stream()
                    .map(entry -> {
                        return new Pair<K, V>(deserialize(keyClass, keySerde, entry.getKey()),
                                deserialize(valueClass, valueSerde, entry.getValue()));
                    }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue()));
        }, () -> Collections.emptyMap());

    }

    @Override
    public <K, V> Map<Long, V> getWindow(String store, Class<K> keyClass, K key, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde, long from, long to) {


        return execute(() -> getUriBuilder()
                .setPath(String.format("/api/v1/window/%s/%s", store, Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", key))))
                .addParameter("keySerde", keySerde.getClass().getName())
                .addParameter("valueSerde", valueSerde.getClass().getName())
                .addParameter("from", String.valueOf(from))
                .addParameter("to", String.valueOf(to))
                .build(), bytes -> {
            WindowedQueryResponse resp = mapper.readValue(bytes, WindowedQueryResponse.class);


            return new TreeMap<Long, V>(resp.getValues().entrySet().stream()
                    .map(entry -> {
                        return new Pair<Long, V>(entry.getKey(),
                                deserialize(valueClass, valueSerde, entry.getValue()));
                    }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue())));
        }, () -> Collections.emptyMap());


    }

    @Override
    public Optional<Long> count(String store) {
        return execute(() -> getUriBuilder()
                .setPath(String.format("/api/v1/kv/%s/count", store))
                .build(), bytes -> {
            Map<String, Object> stringObjectMap = mapper.readValue(bytes, Map.class);

            Object count = stringObjectMap.get("count");
            if(count instanceof Integer){
                return Optional.of(((Integer) count).longValue());
            } else {
                return Optional.of((Long) count);
            }


        }, () -> Optional.empty());
    }

    @Override
    public <K, V> Map<Window, V> getSession(String store, Class<K> keyClass, K key, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde) {
        return execute(() -> getUriBuilder()
                .setPath(String.format("/api/v1/session/%s/%s", store, Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", key))))
                .addParameter("keySerde", keySerde.getClass().getName())
                .addParameter("valueSerde", valueSerde.getClass().getName())
                .build(), bytes -> {
            SessionQueryResponse resp = mapper.readValue(bytes, SessionQueryResponse.class);






            return new TreeMap<Window, V>(resp.getValues().stream()
                    .map(window -> {
                        return new Pair<Window, V>(window,
                                deserialize(valueClass, valueSerde,window.getValue()));
                    }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue())));
        }, () -> Collections.emptyMap());
    }


    private <U> U execute(URISupplier<URI> uriSupplier, MappingFunction<byte[], U> responseMapper, Supplier<U> notFoundMapper) {
        try {
            URI uri = uriSupplier.get();
            Request request = Request.Get(uri);
            HttpResponse response = request.execute().returnResponse();

            if (response.getStatusLine().getStatusCode() == 200) {
                byte[] returnJson = EntityUtils.toByteArray(response.getEntity());

                return responseMapper.apply(returnJson);


            } else if (response.getStatusLine().getStatusCode() == 404) {
                return notFoundMapper.get();
            } else if (response.getStatusLine().getStatusCode() == 400) {
                throw new IllegalArgumentException("Bad Request");
            } else {
                throw new QueryExecutionException("Something went wrong, status code: " + response.getStatusLine().getStatusCode());
            }


        } catch (URISyntaxException | IOException e) {
            throw new ConnectionException("Error creating connection", e);
        }

    }


    private URIBuilder getUriBuilder() {
        return new URIBuilder().setScheme("http").setHost(host).setPort(port);
    }


    private <T> T deserialize(Class<T> clazz, Serde<T> serde, String b64Representation) {
        byte[] bytes = Base64.getDecoder().decode(b64Representation);
        return serde.deserializer().deserialize("", bytes);
    }

    private static final class Pair<K, V> {
        private final K key;
        private final V value;

        public Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }

        public K getKey() {
            return key;
        }

        public V getValue() {
            return value;
        }
    }


}
