
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for abstract class RideShareStreamerBase                //
//                                                                      //
//  Description: Provides the base for the Kafka stream processors      //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

package com.insight;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.json.JSONException;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public abstract class RideShareStreamerBase {

    static final String TOPICIN = "ride-share-input-test8";
    static final String TOPICOUT = "ride-share-output-test8";

    void processStream() {
        StreamsConfig config = initConfig();

        // start kafka streams.
        KafkaStreams streams = new KafkaStreams(buildStream(TOPICIN, TOPICOUT), config);
        streams.start();

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    static Topology buildStream( String topicin, String topiout ) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> rideStream = builder.stream(topicin);

        KStream<String, GenericRecord> processedStream = rideStream.mapValues(val -> {
            try {
                return processMessage(val);
            } catch (JSONException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            return val;
        });

        processedStream.to(topiout);

        return builder.build();
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
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "rides-test8");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerDNS1 + ":9092," + brokerDNS2 + ":9092,"
                + brokerDNS3 + ":9092," + brokerDNS4 + ":9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, genericAvroSerde.getClass().getName());

        return new StreamsConfig(props);
    }

    static GenericRecord processMessage(GenericRecord val) throws JSONException, IOException {
        return val;
    }

}
