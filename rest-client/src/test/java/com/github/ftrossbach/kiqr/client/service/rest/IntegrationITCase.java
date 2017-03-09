package com.github.ftrossbach.kiqr.client.service.rest;

import com.github.ftrossbach.kiqr.client.service.BlockingKiqrService;
import com.github.ftrossbach.kiqr.client.service.QueryExecutionException;
import com.github.ftrossbach.kiqr.core.RuntimeVerticle;
import com.github.ftrossbach.kiqr.rest.server.HttpServer;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;


/**
 * Created by ftr on 08/03/2017.
 */
public class IntegrationITCase {


    private final static String KAFKA_HOST = "localhost";
    private final static String KAFKA_PORT;
    static {
        if(System.getenv("KAFKA_PORT") != null){
            KAFKA_PORT = System.getenv("KAFKA_PORT");
        } else {
            KAFKA_PORT = "9092";
        }
    }

    private static String TOPIC = UUID.randomUUID().toString();

    private static final Vertx VERTX = Vertx.vertx();

    @BeforeClass
    public static void produceMessages() throws Exception{
        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", KAFKA_HOST + ":" + KAFKA_PORT);
        producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producerProps.put("value.serializer", "org.apache.kafka.common.serialization.LongSerializer");
        producerProps.put("linger.ms", 0);


        try(KafkaProducer<String, Long> producer = new KafkaProducer<>(producerProps)){
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 0L, "key1", 1L));
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 100L, "key1", 2L));
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 100000L, "key1", 3L));


            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 0L, "key2", 4L));
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 100000L, "key2", 5L));
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 100001L, "key2", 6L));

            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 0L, "key3", 7L));
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 50000L, "key3", 8L));
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 100001L, "key3", 9L));


            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 0L, "key4", 10L));
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 1L, "key4", 11L));
            producer.send(new ProducerRecord<String, Long>(TOPIC, 0, 2L, "key4", 12L));

        }

        CountDownLatch cdl = new CountDownLatch(12);


        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers",  KAFKA_HOST + ":" + KAFKA_PORT);
        consumerProps.put("group.id", UUID.randomUUID().toString());
        consumerProps.put("enable.auto.commit", "true");
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");


        Runnable consumerRunnable = () -> {
            KafkaConsumer<String, Long> consumer = new KafkaConsumer<>(consumerProps);

            consumer.subscribe(Collections.singleton(TOPIC));

            int tryCount = 10;
            while(true){
                ConsumerRecords<String, Long> records = consumer.poll(500);
                records.forEach(rec -> cdl.countDown());

                tryCount--;
                if(cdl.getCount() == 0){
                    consumer.close();
                    return;
                } else if(tryCount == 0){
                    throw new RuntimeException("times up");
                }
            }
        };

        consumerRunnable.run();

        cdl.await(10000, TimeUnit.MILLISECONDS);


        KStreamBuilder builder = new KStreamBuilder();
        KTable<String, Long> kv = builder.table(Serdes.String(), Serdes.Long(), TOPIC, "kv");

        kv.toStream().groupByKey().count(TimeWindows.of(10000L), "window");

        Properties streamProps = new Properties();
        streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  KAFKA_HOST + ":" + KAFKA_PORT);
        streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamProps.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamProps.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

        CountDownLatch streamCdl = new CountDownLatch(2);


        HttpServer.Builder verticleBuilder = new HttpServer.Builder(builder, streamProps);
        RuntimeVerticle.Builder builder1 = verticleBuilder.withPort(44321).withStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING) streamCdl.countDown();
            System.out.println(oldState + " - " + newState);
        });

        AbstractVerticle verticle = verticleBuilder.build();

        VERTX.deployVerticle(verticle);

        streamCdl.await(10000, TimeUnit.MILLISECONDS);

    }

    @Test
    public void successfulScalarQuery() throws Exception{



        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Optional<Long> resultKey1 = client.getScalarKeyValue("kv", String.class, "key1", Long.class, Serdes.String(), Serdes.Long());
        assertTrue(resultKey1.isPresent());
        assertThat(resultKey1.get(), is(equalTo(3L)));

        Optional<Long> resultKey2 = client.getScalarKeyValue("kv", String.class, "key3", Long.class, Serdes.String(), Serdes.Long());
        assertTrue(resultKey2.isPresent());
        assertThat(resultKey2.get(), is(equalTo(9L)));

    }

    @Test
    public void notFoundScalarQuery() throws Exception{



        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Optional<Long> resultKey1 = client.getScalarKeyValue("kv", String.class, "key5", Long.class, Serdes.String(), Serdes.Long());
        assertFalse(resultKey1.isPresent());

    }

    @Test
    public void noSuchStoreScalarQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Optional<Long> resultKey1 = client.getScalarKeyValue("idontexist", String.class, "key1", Long.class, Serdes.String(), Serdes.Long());
        assertFalse(resultKey1.isPresent());

    }

    @Test(expected = QueryExecutionException.class)
    public void wrongStoreTypeScalarQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Optional<Long> resultKey1 = client.getScalarKeyValue("window", String.class, "key1", Long.class, Serdes.String(), Serdes.Long());


    }

    @Test
    public void successfulAllQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<String, Long> result = client.getAllKeyValues("kv", String.class, Long.class, Serdes.String(), Serdes.Long());
        assertThat(result.entrySet(),hasSize(4));
        assertThat(result, hasEntry("key1", 3L));
        assertThat(result, hasEntry("key2", 6L));
        assertThat(result, hasEntry("key3", 9L));
        assertThat(result, hasEntry("key4", 12L));

    }


    @Test
    public void noSuchStoreAllQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<String, Long> result = client.getAllKeyValues("idontexist", String.class, Long.class, Serdes.String(), Serdes.Long());
        assertTrue(result.isEmpty());


    }


    @Test(expected = QueryExecutionException.class)
    public void wrongStoreTypeAllQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<String, Long> result = client.getAllKeyValues("window", String.class, Long.class, Serdes.String(), Serdes.Long());


    }

    @Test
    public void successfulRangeQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<String, Long> result = client.getRangeKeyValues("kv", String.class, Long.class, Serdes.String(), Serdes.Long(), "key1", "key2");
        assertThat(result.entrySet(),hasSize(2));
        assertThat(result, hasEntry("key1", 3L));
        assertThat(result, hasEntry("key2", 6L));

    }

    @Test
    public void emptyRangeQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<String, Long> result = client.getRangeKeyValues("kv", String.class, Long.class, Serdes.String(), Serdes.Long(), "key6", "key7");
        assertThat(result.entrySet(),is(empty()));

    }

    @Test(expected = QueryExecutionException.class)
    public void invertedRangeQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<String, Long> result = client.getRangeKeyValues("kv", String.class, Long.class, Serdes.String(), Serdes.Long(), "key3", "key1");
        assertThat(result.entrySet(),is(empty()));

    }

    @Test
    public void noSuchStoreRangeQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<String, Long> result = client.getRangeKeyValues("idontexist", String.class, Long.class, Serdes.String(), Serdes.Long(), "key1", "key2");
        assertTrue(result.isEmpty());


    }

    @Test(expected = QueryExecutionException.class)
    public void wrongStoreTypeRangeQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<String, Long> result = client.getRangeKeyValues("window", String.class, Long.class, Serdes.String(), Serdes.Long(), "key1", "key2");



    }

    @Test
    public void successfulWindowQuery() throws Exception{

        BlockingKiqrService client = new BlockingRestKiqrServiceImpl("localhost", 44321);

        Map<Long, Long> result = client.getWindow("window", String.class, "key1", Long.class, Serdes.String(), Serdes.Long(), 0L, 100001L);
        assertThat(result.entrySet(),hasSize(2));
        assertThat(result, hasEntry(0L, 2L));
        assertThat(result, hasEntry(100000L, 1L));

        Map<Long, Long> resultKey2 = client.getWindow("window", String.class, "key2", Long.class, Serdes.String(), Serdes.Long(), 0L, 100001L);
        assertThat(resultKey2.entrySet(),hasSize(2));
        assertThat(resultKey2, hasEntry(0L, 1L));
        assertThat(resultKey2, hasEntry(100000L, 2L));

        Map<Long, Long> resultKey3 = client.getWindow("window", String.class, "key3", Long.class, Serdes.String(), Serdes.Long(), 0L, 100001L);
        assertThat(resultKey3.entrySet(),hasSize(3));
        assertThat(resultKey3, hasEntry(0L, 1L));
        assertThat(resultKey3, hasEntry(50000L, 1L));
        assertThat(resultKey3, hasEntry(100000L, 1L));

        Map<Long, Long> resultKey4 = client.getWindow("window", String.class, "key4", Long.class, Serdes.String(), Serdes.Long(), 0L, 100001L);
        assertThat(resultKey4.entrySet(),hasSize(1));
        assertThat(resultKey4, hasEntry(0L, 3L));


    }

    @AfterClass
    public static void tearDown() throws Exception{
        CountDownLatch cdl = new CountDownLatch(1);

        VERTX.close(handler -> cdl.countDown());

        cdl.await(30000, TimeUnit.MILLISECONDS);
    }



}
