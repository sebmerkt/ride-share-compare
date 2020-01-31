package io.confluent.examples.clients.basicavro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerExample {

    private static final String TOPIC = "taxiinput";

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {
        final Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "insight-ride-share-application");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-payments");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

//        final Serde stringSerde = Serdes.String();

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

//            while (true) {
//                final ConsumerRecords<String, String> records = consumer.poll(100);
////                System.out.println(records);
//                for (final ConsumerRecord<String, String> record : records) {
//                    final String key = record.key();
//                    final String value = record.value();
//                    System.out.printf("key = %s, value = %s%n", key, value);
//                }
//            }

        }




//        final KStreamBuilder builder = new KStreamBuilder();
//        final KStream<String, String> inputStreamData = builder.stream(stringSerde, stringSerde, "taxiinput");
//
//        final KStream<String, String> processedStream = inputStreamData.mapValues(record -> record.toUpperCase() );
//
//        processedStream.to(stringSerde, stringSerde, "taxioutput");
////
//        final KafkaStreams streams = new KafkaStreams(builder, props);
//        streams.cleanUp();
//        streams.start();
//
//        // print the topology
//        System.out.println(streams.toString());
//
//        // shutdown hook to correctly close the streams application
//        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

//        try (final KafkaConsumer<String, Payment> consumer = new KafkaConsumer<>(props)) {
//            consumer.subscribe(Collections.singletonList(TOPIC));
//
//            while (true) {
//                final ConsumerRecords<String, Payment> records = consumer.poll(100);
//                for (final ConsumerRecord<String, Payment> record : records) {
//                    final String key = record.key();
//                    final Payment value = record.value();
//                    System.out.printf("key = %s, value = %s%n", key, value);
//                }
//            }
//
//        }
//    }
}
