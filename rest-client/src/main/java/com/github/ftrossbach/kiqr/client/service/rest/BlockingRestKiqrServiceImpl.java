package com.github.ftrossbach.kiqr.client.service.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.ftrossbach.kiqr.client.service.BlockingKiqrService;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.MultiValuedKeyValueQueryResponse;
import com.github.ftrossbach.kiqr.commons.config.querymodel.requests.QueryStatus;
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

                if (resp.getStatus() == QueryStatus.OK) {
                    return Optional.of(deserialize(valueClass, valueSerde, resp.getValue()));
                } else {
                    return Optional.empty();
                }

            } else {
                return Optional.empty();
            }


        } catch (URISyntaxException e) {
            throw new RuntimeException("Error constructing endpoint", e);
        } catch (ClientProtocolException e) {
            throw new RuntimeException("Error constructing request", e);
        } catch (IOException e) {
            throw new RuntimeException("Error executing endpoint", e);
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

                if (resp.getStatus() == QueryStatus.OK) {

                    return resp.getResults().entrySet().stream()
                            .map(entry -> {
                                return new Pair<K, V>(deserialize(keyClass, keySerde,entry.getKey()),
                                        deserialize(valueClass, valueSerde, entry.getValue()));
                            }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue()));


                } else {
                    return Collections.emptyMap();
                }

            } else {
                return Collections.emptyMap();
            }


        } catch (URISyntaxException e) {
            throw new RuntimeException("Error constructing endpoint", e);
        } catch (ClientProtocolException e) {
            throw new RuntimeException("Error constructing request", e);
        } catch (IOException e) {
            throw new RuntimeException("Error executing endpoint", e);
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

                if (resp.getStatus() == QueryStatus.OK) {

                    return resp.getResults().entrySet().stream()
                            .map(entry -> {
                                return new Pair<K, V>(deserialize(keyClass, keySerde,entry.getKey()),
                                        deserialize(valueClass, valueSerde, entry.getValue()));
                            }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue()));


                } else {
                    return Collections.emptyMap();
                }

            } else {
                return Collections.emptyMap();
            }


        } catch (URISyntaxException e) {
            throw new RuntimeException("Error constructing endpoint", e);
        } catch (ClientProtocolException e) {
            throw new RuntimeException("Error constructing request", e);
        } catch (IOException e) {
            throw new RuntimeException("Error executing endpoint", e);
        }
    }

    @Override
    public <K, V> Map<Long, V> getWindow(String store, Class<K> keyClass, K key, Class<V> valueClass, Serde<K> keySerde, Serde<V> valueSerde, long from, long to) {
        try {
            URI uri = getUriBuilder()
                    .setPath(String.format("/api/v1/window/%s/%s", store,Base64.getEncoder().encodeToString(keySerde.serializer().serialize("", key))))
                    .addParameter("keySerde", keySerde.getClass().getName())
                    .addParameter("valueSerde", valueSerde.getClass().getName())
                    .addParameter("from",  String.valueOf(from))
                    .addParameter("to", String.valueOf(to))
                    .build();
            Request request = Request.Get(uri);
            HttpResponse response = request.execute().returnResponse();

            if (response.getStatusLine().getStatusCode() == 200) {
                byte[] returnJson = EntityUtils.toByteArray(response.getEntity());

                WindowedQueryResponse resp = mapper.readValue(returnJson, WindowedQueryResponse.class);

                if (resp.getStatus() == QueryStatus.OK) {

                    return new TreeMap<Long, V>(resp.getValues().entrySet().stream()
                            .map(entry -> {
                                return new Pair<Long, V>(entry.getKey(),
                                        deserialize(valueClass, valueSerde, entry.getValue()));
                            }).collect(Collectors.toMap(Pair::getKey, pair -> pair.getValue())));


                } else {
                    return Collections.emptyMap();
                }

            } else {
                return Collections.emptyMap();
            }


        } catch (URISyntaxException e) {
            throw new RuntimeException("Error constructing endpoint", e);
        } catch (ClientProtocolException e) {
            throw new RuntimeException("Error constructing request", e);
        } catch (IOException e) {
            throw new RuntimeException("Error executing endpoint", e);
        }

    }


    private URIBuilder getUriBuilder() {
        return new URIBuilder().setScheme("http").setHost(host).setPort(port);
    }


    private <T> T deserialize(Class<T> clazz, Serde<T> serde, String b64Representation){
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
