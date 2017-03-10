[![Build Status](https://travis-ci.org/ftrossbach/kiqr.svg?branch=master)](https://travis-ci.org/ftrossbach/kiqr)
[![Coverage Status](https://coveralls.io/repos/github/ftrossbach/kiqr/badge.svg)](https://coveralls.io/github/ftrossbach/kiqr)

#KIQR - Kafka Interactive Query Runtime

This project aims at providing a general purpose runtime for interactive queries.
It uses Vert.x for coordination between cluster instances.


## Why
Apache Kafka has a cool feature called "Interactive Queries" that enables you to query the internal state of a 
Kafka Streams application. That's pretty cool, but if you run your streams application in a distributed manner where 
different instances of your app are assigned different partitions of your inbound Kafka topics, each
instance is only aware of the messages that come it's way. If you want a reliable query environment, you need to build
a layer that is aware of those instances and which instances is responsible for which key. 

KIQR was started as a vehicle to get deeper into the Interactive Query feature and to pick up some Vert.x knowledge
along the way. It probably will never get past that point, but I'll be happy if it is of any use to someone else.

## How
The property "application.server" in KafkaStreams lets each instance share information of its coordinates on the 
network in host:port format with all other instances via Kafka protocol mechanisms. KIQR uses this feature, but in
a different way. On startup, instances assign themselves a UUID as host (and a irrelevant value for port). It then
uses this UUID to register at Vert.x' event bus. If you run Vert.x in cluster mode, this will be a distributed event
bus, meaning that the instances can talk to each other on that bus. So any instance can query one of KafkaStreams'
metadata methods and know at which address on the event bus to direct the query at. You can use any of Vert.x' supported
cluster mechanisms.

For serialization purposes, Kafka's Serdes (Serializer/Deserializers) are used as they are required to interact with
Kafka anyway. They need to be on the classpath of both client and server.

## Client 
At the moment, KIQR allows queries via HTTP. There is a server and a client module. More clients are certainly imaginable.

## Examples


### Server Runtime
Running a streams application in the KIQR runtime
```
Properties streamProps = new Properties();
streamProps.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
streamProps.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,  "my-streaming-app");
streamProps.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
streamProps.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
streamProps.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass().getName());

KStreamBuilder builder = new KStreamBuilder();
KTable<String, Long> kv = builder.table(Serdes.String(), Serdes.Long(), TOPIC, "kv");
kv.toStream().groupByKey().count(TimeWindows.of(10000L), "window");

Vertx vertx = ...;// get your vertx from somewhere

RestKiqrServerVerticle.Builder verticleBuilder = RestKiqrServerVerticle.Builder.serverBuilder(builder, streamProps).withPort(PORT2);

vertx.deploy(verticleBuilder.build());

```

There are multiple ways to get hold of the Vertx object.
If you run in a clustered environment, check out the [Vert.x docs on clustering](http://vertx.io/docs/#clustering).
For single instance tests, it can be as easy as calling
```
Vertx vertx = Vertx.vertx();
```

### Client
There is a rest client that does all the deserialization for you, so you only interact with the actual data types and
not some serialized byte arrays. The client is written plain Java without Vert.x. 
It only depends on Kafka Streams (for the serdes), Apache's HTTP client and Jackson.

There is a generic variant were you specify the class and serde of both key and value type on each call, and a specific 
one that can only be used for one key-value combination but gets these set in its constructor.


```
GenericBlockingKiqrClient client = new GenericBlockingRestKiqrClientImpl("localhost", port);

//querying key "key1" from key-value store "kv" with String keys and Long values
Optional<Long> result = client.getScalarKeyValue("kv", String.class, "key1", Long.class, Serdes.String(), Serdes.Long());

//querying all keys from store "kv" with String keys and Long values
Map<String, Long> result = client.getAllKeyValues("kv", String.class, Long.class, Serdes.String(), Serdes.Long());

//querying key range "key1" to "key3" from store "kv" with String keys and Long values
Map<String, Long> result = client.getRangeKeyValues("kv", String.class, Long.class, Serdes.String(), Serdes.Long(), "key1", "key3");

//querying windows for "key1" from epoch time 1 to epoch time 1000 from store "window" with String keys and Long values
Map<Long, Long> result = client.getWindow("window", String.class, "key1", Long.class, Serdes.String(), Serdes.Long(), 1L, 1000L);
```

Those methods look a bit clunky, that's why there is a specific variant:

```
//constructing a client for a store called "kv" with String keys and long values
SpecificBlockingKiqrClient<String, Long> client = new SpecificBlockingRestKiqrClientImpl<>("localhost", 44321, "kv", String.class, Long.class, Serdes.String(), Serdes.Long());

//querying key "key1" from key-value store "kv" with String keys and Long values
Optional<Long> result = client.getScalarKeyValue("key1");

//querying all keys from store "kv" with String keys and Long values
Map<String, Long> result = client.getAllKeyValues();

//querying key range "key1" to "key3" from store "kv" with String keys and Long values
Map<String, Long> result = client.getRangeKeyValues("key1", "key3");

//querying windows for "key1" from epoch time 1 to epoch time 1000 from store "window" with String keys and Long values
Map<Long, Long> result = client.getWindow("key1", 1L, 1000L);
       
```

## Caveats and restrictions

* No support for Session Window queries 
* not very well integrationally tested (yet? it is a hobby project)
* not HA (when the streams app is rebalancing, there is not much you can do at this point)
* you can probably break things by querying all data from a kv store
* highly unstable API and implementation, things will change
* you are responsible to know the names of the state stores and types of your keys and values in Kafka. There is 
no way to infer them at runtime.
* Java 8+
* Kafka Streams 0.10.2



