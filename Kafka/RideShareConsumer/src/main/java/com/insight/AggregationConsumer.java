package com.insight;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;

public class AggregationConsumer {


    static final String TOPIC = "taxitestagg";

    static Map<String, String> env = System.getenv();

    Properties props = null;

    Connection dbConn = null;

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        AggregationConsumer aggregationConsumer = new AggregationConsumer();
//        aggregationConsumer.connect();
        Properties props = initProperties();
//        aggregationConsumer.writeToDB();

        final KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        while (true) {
            final ConsumerRecords<String, Integer> record = consumer.poll(ofMillis(10));
//            final String test = record;
            System.out.println(record);
        }

    }


//    void writeToDB(){
//        try (final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
//            consumer.subscribe(Collections.singletonList(TOPIC));
//
//            while (true) {
//                final ConsumerRecords<String, GenericRecord> records = consumer.poll(ofMillis(10));
//                for (final ConsumerRecord<String, GenericRecord> record : records) {
//
//                    final String test = record.value().get("test").toString();
//
//                    Statement stmt = dbConn.createStatement();
//
//                    String sql = "INSERT INTO aggregations " +
//                            "VALUES ('" + test+")";
//
//                    stmt.executeUpdate(sql);
//                }
//            }
//
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//    }



    public static Properties initProperties() {

        String schemaDNS = env.get("SCHEMA_REGISTRY");
        String brokerDNS1 = env.get("BROKER1");
        String brokerDNS2 = env.get("BROKER2");
        String brokerDNS3 = env.get("BROKER3");
        String brokerDNS4 = env.get("BROKER4");


        String schemaUrl = "http://" + schemaDNS + ":8081";

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                brokerDNS1 + ":9092," + brokerDNS2 + ":9092," + brokerDNS3 + ":9092," + brokerDNS4 + ":9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-rides");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        //        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        return props;
    }

//    public Connection connect() {
//
//        String dbServer = env.get("DB_SERVER");
//        String dbName = env.get("DB_NAME");
//
//        String url = "jdbc:postgresql://" + dbServer + "/" + dbName;
//        String user = env.get("DB_USER");
//        String password = env.get("DB_PW");
//
//        try {
//            dbConn = DriverManager.getConnection(url, user, password);
//            System.out.println("Connected to the PostgreSQL server successfully.");
//        } catch (SQLException e) {
//            System.out.println(e.getMessage());
//        }
//        return dbConn;
//    }

}
