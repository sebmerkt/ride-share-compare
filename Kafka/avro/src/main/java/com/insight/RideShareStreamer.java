package com.insight;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import static com.insight.RideShareProducer.buildRecord;

public class RideShareStreamer {
    public static void main(String[] args) throws Exception {

        final String TOPICIN = "taxitest3in";
        final String TOPICOUT = "taxitest3out";

        final Map<String, String> env = System.getenv();
        final String schemaDNS = env.get("SCHEMA_REGISTRY");
        final String brokerDNS1 = env.get("BROKER1");
        final String brokerDNS2 = env.get("BROKER2");
        final String brokerDNS3 = env.get("BROKER3");
        final String brokerDNS4 = env.get("BROKER4");

        final String schemaUrl = "http://" + schemaDNS + ":8081";

        // avro schema avsc file path.
        final String schemaPath = "src/main/resources/avro/com/insight/yellowcab.avsc";

        final Serde<GenericRecord> genericAvroSerde = new GenericAvroSerde();

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "ride-share-stream-processing");
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "test-rides");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokerDNS1 + ":9092," + brokerDNS2 + ":9092,"
                + brokerDNS3 + ":9092," + brokerDNS4 + ":9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, genericAvroSerde.getClass().getName());
        StreamsConfig config = new StreamsConfig(props);

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> rideStream = builder.stream(TOPICIN);
        ;
        KStream<String, GenericRecord> processedStream = rideStream.mapValues(val -> {

//            final File schemaFile = new File(schemaPath);
//            Schema avroSchema = null;
//            try {
//                avroSchema = new Schema.Parser().parse(schemaFile);
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            val.toString().split(",", -18))[11];

            // message value, avro generic record.
//            GenericRecord pageViewEventRecord = buildRecord(avroSchema, );

            System.out.println(val.get("Payment_Type"));
            val.put("Payment_Type", val.get("Payment_Type")+"-NEW");
            return val;
        });

        rideStream.to(TOPICOUT);

        // start kafka streams.
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();


        // print the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
