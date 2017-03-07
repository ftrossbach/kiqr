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
package com.github.ftrossbach.kiqr.client.service.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ftrossbach.kiqr.client.service.BlockingKiqrService;
import com.github.ftrossbach.kiqr.client.service.ConnectionException;
import com.github.ftrossbach.kiqr.client.service.QueryExecutionException;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.MultiValuedKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.ScalarKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.WindowedQueryResponse;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.serialization.Serde;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by ftr on 01/03/2017.
 */
public class BlockingRestKiqrServiceImpl implements BlockingKiqrService {

    private final String host;
    private final int port;
    private final ObjectMapper mapper;

    public BlockingRestKiqrServiceImpl(String host, int port) {
        this.host = host;
        this.port = port;
        mapper = new ObjectMapper();
    }

    @Override
    public <K, V> Optional<V> getScalarKeyValue(String store, Class<K> keyClass, K key, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde) {


        try {
            URI uri = getUriBuilder()
                    .setPath(String.format("/api/v1/kv/%s/%s", store, Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", key))))
                    .addParameter("keySerde", keySerde.getClass().getName())
                    .addParameter("valueSerde", valueSerde.getClass().getName())
                    .build();
            Request request = Request.Get(uri);
            HttpResponse response = request.execute().returnResponse();

            if (response.getStatusLine().getStatusCode() == 200) {
                byte[] returnJson = EntityUtils.toByteArray(response.getEntity());

                ScalarKeyValueQueryResponse resp = mapper.readValue(returnJson, ScalarKeyValueQueryResponse.class);


                return Optional.of(deserialize(valueClass, valueSerde, resp.getValue()));


            } else if (response.getStatusLine().getStatusCode() == 404) {
                return Optional.empty();
            } else if (response.getStatusLine().getStatusCode() == 400) {
                throw new IllegalArgumentException("Bad Request");
            } else {
                throw new QueryExecutionException("Something went wrong, status code: " + response.getStatusLine().getStatusCode());
            }


        } catch (URISyntaxException | IOException e) {
            throw new ConnectionException("Error creating connection", e);
        }


    }

    @Override
    public <K, V> Map<K, V> getAllKeyValues(String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde) {
        try {
            URI uri = getUriBuilder()
                    .setPath(String.format("/api/v1/kv/%s", store))
                    .addParameter("keySerde", keySerde.getClass().getName())
                    .addParameter("valueSerde", valueSerde.getClass().getName())
                    .build();
            Request request = Request.Get(uri);
            HttpResponse response = request.execute().returnResponse();

            if (response.getStatusLine().getStatusCode() == 200) {
                byte[] returnJson = EntityUtils.toByteArray(response.getEntity());

                MultiValuedKeyValueQueryResponse resp = mapper.readValue(returnJson, MultiValuedKeyValueQueryResponse.class);


                return resp.getResults().entrySet().stream()
                        .map(entry -> {
                            return new Pair<K, V>(deserialize(keyClass, keySerde, entry.getKey()),
                                    deserialize(valueClass, valueSerde, entry.getValue()));
                        }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue()));


            } else if (response.getStatusLine().getStatusCode() == 404) {
                return Collections.emptyMap();
            } else if (response.getStatusLine().getStatusCode() == 400) {
                throw new IllegalArgumentException("Bad Request");
            } else {
                throw new QueryExecutionException("Something went wrong, status code: " + response.getStatusLine().getStatusCode());
            }


        } catch (URISyntaxException | IOException e) {
            throw new ConnectionException("Error creating connection", e);
        }

    }

    @Override
    public <K, V> Map<K, V> getRangeKeyValues(String store, Class<K> keyClass, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde, K from, K to) {
        try {
            URI uri = getUriBuilder()
                    .setPath(String.format("/api/v1/kv/%s", store))
                    .addParameter("keySerde", keySerde.getClass().getName())
                    .addParameter("valueSerde", valueSerde.getClass().getName())
                    .addParameter("from", Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", from)))
                    .addParameter("to", Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", to)))
                    .build();
            Request request = Request.Get(uri);
            HttpResponse response = request.execute().returnResponse();

            if (response.getStatusLine().getStatusCode() == 200) {
                byte[] returnJson = EntityUtils.toByteArray(response.getEntity());

                MultiValuedKeyValueQueryResponse resp = mapper.readValue(returnJson, MultiValuedKeyValueQueryResponse.class);


                return resp.getResults().entrySet().stream()
                        .map(entry -> {
                            return new Pair<K, V>(deserialize(keyClass, keySerde, entry.getKey()),
                                    deserialize(valueClass, valueSerde, entry.getValue()));
                        }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue()));


            } else if (response.getStatusLine().getStatusCode() == 404) {
                return Collections.emptyMap();
            } else if (response.getStatusLine().getStatusCode() == 400) {
                throw new IllegalArgumentException("Bad Request");
            } else {
                throw new QueryExecutionException("Something went wrong, status code: " + response.getStatusLine().getStatusCode());
            }


        } catch (URISyntaxException | IOException e) {
            throw new ConnectionException("Error creating connection", e);
        }
    }

    @Override
    public <K, V> Map<Long, V> getWindow(String store, Class<K> keyClass, K key, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde, long from, long to) {
        try {
            URI uri = getUriBuilder()
                    .setPath(String.format("/api/v1/window/%s/%s", store, Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", key))))
                    .addParameter("keySerde", keySerde.getClass().getName())
                    .addParameter("valueSerde", valueSerde.getClass().getName())
                    .addParameter("from", String.valueOf(from))
                    .addParameter("to", String.valueOf(to))
                    .build();
            Request request = Request.Get(uri);
            HttpResponse response = request.execute().returnResponse();

            if (response.getStatusLine().getStatusCode() == 200) {
                byte[] returnJson = EntityUtils.toByteArray(response.getEntity());

                WindowedQueryResponse resp = mapper.readValue(returnJson, WindowedQueryResponse.class);


                return new TreeMap<Long, V>(resp.getValues().entrySet().stream()
                        .map(entry -> {
                            return new Pair<Long, V>(entry.getKey(),
                                    deserialize(valueClass, valueSerde, entry.getValue()));
                        }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue())));


            } else if (response.getStatusLine().getStatusCode() == 404) {
                return Collections.emptyMap();
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
