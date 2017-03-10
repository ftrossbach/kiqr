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

## Client 
At the moment, KIQR allows queries via HTTP. There is a server and a client module. More clients are certainly imaginable.

## Examples

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

## Caveats
This is
* not very well integrationally tested (it is a hobby project)
* not HA (when the streams app is rebalancing, there is not much you can do at this point)
* you can probably break things by querying all data from a kv store
* highly unstable API and implementation, things will change



