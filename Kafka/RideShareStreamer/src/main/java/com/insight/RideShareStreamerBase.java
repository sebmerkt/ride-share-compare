package com.insight;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.time.Duration.ofSeconds;

public abstract class RideShareStreamerBase {

    static final String TOPICIN = "taxitest4in";
    static final String TOPICOUT = "taxitest4out";

    void processStream() {
        StreamsConfig config = initConfig();

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> rideStream = builder.stream(TOPICIN);

        KStream<String, GenericRecord> processedStream = rideStream.mapValues(val -> processMessage(val));


// TESTING
//        final Map<String, String> env = System.getenv();
//
//        final String brokerDNS1 = env.get("BROKER1");
//        final String brokerDNS2 = env.get("BROKER2");
//        final String brokerDNS3 = env.get("BROKER3");
//        final String brokerDNS4 = env.get("BROKER4");
//
//        Properties props2 = new Properties();
//        props2.put(StreamsConfig.APPLICATION_ID_CONFIG, "ride-share-stream-processing");
//        props2.put(StreamsConfig.CLIENT_ID_CONFIG, "test-rides");
//        props2.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerDNS1 + ":9092," + brokerDNS2 + ":9092,"
//                + brokerDNS3 + ":9092," + brokerDNS4 + ":9092");
//        props2.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
//        props2.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass().getName());
//
//        StreamsConfig config2 =  new StreamsConfig(props2);
//
//
//        KStream<String, Integer> testStream = processedStream.map((k, v) -> new KeyValue<String, Integer>(k, 1));
//
////        KTable<String, Long> testTable = testStream.groupByKey()
////                .count();
//
//        KStream<String, Integer> testAgg = testStream.groupByKey()
//                .reduce((aggValue, newValue) -> aggValue + newValue, Materialized.as("SALES_STORE"))
//                .toStream();
//
//        // Write KStream to a topic
//        testAgg.to("taxitestagg");
//
//        // start kafka streams.
//        KafkaStreams streams2 = new KafkaStreams(builder.build(), config2);
//        streams2.start();
//
//
////        KTable<Windowed<String>, GenericRecord> oneMinuteWindowed = processedStream
////
////                .groupByKey()
////
////                .reduce((val, agg) -> agg + Double.valueOf(val.get("Total_Amt").toString()), TimeWindows.of(ofSeconds(60)), "store1m");

// TESTING

        processedStream.to(TOPICOUT);

        // start kafka streams.
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static StreamsConfig initConfig() {
        final Map<String, String> env = System.getenv();
        final String schemaDNS = env.get("SCHEMA_REGISTRY");
        final String brokerDNS1 = env.get("BROKER1");
        final String brokerDNS2 = env.get("BROKER2");
        final String brokerDNS3 = env.get("BROKER3");
        final String brokerDNS4 = env.get("BROKER4");

        final String schemaUrl = "http://" + schemaDNS + ":8081";

        final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ride-share-stream-processing");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "test-rides");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerDNS1 + ":9092," + brokerDNS2 + ":9092,"
                + brokerDNS3 + ":9092," + brokerDNS4 + ":9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, genericAvroSerde.getClass().getName());

        return new StreamsConfig(props);
    }

    abstract GenericRecord processMessage(GenericRecord val);

}
