package com.insight;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

abstract public class RideShareConsumerBase {

    static final String TOPIC = "taxitest13out";

    static Map<String, String> env = System.getenv();

    Properties props = null;

    Connection dbConn = null;

    abstract void writeToDB();

    public void initProperties() {

        String schemaDNS = env.get("SCHEMA_REGISTRY");
        String brokerDNS1 = env.get("BROKER1");
        String brokerDNS2 = env.get("BROKER2");
        String brokerDNS3 = env.get("BROKER3");
        String brokerDNS4 = env.get("BROKER4");


        String schemaUrl = "http://" + schemaDNS + ":8081";

        props = new Properties();
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
    }

    public Connection connect() {

        String dbServer = env.get("DB_SERVER");
        String dbName = env.get("DB_NAME");

        String url = "jdbc:postgresql://" + dbServer + "/" + dbName;
        String user = env.get("DB_USER");
        String password = env.get("DB_PW");

//        Connection conn = null;
        try {
            dbConn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return dbConn;
    }

    public static String InsertString(final Object input){
        if (input != null) {
            try {
                return input.toString();
            }
            catch (NumberFormatException e) {
                return "";
            }
        }
        else{
            return "";
        }
    }

    public static double InsertDouble(final Object input){
        if (input != null) {
            try {
                return Double.parseDouble(input.toString());
            }
            catch (NumberFormatException e) {
                return 0.0;
            }
        }
        else{
            return 0.0;
        }
    }

    public static int InsertInt(final Object input){
        if (input != null) {
            try {
                return Integer.parseInt(input.toString());
            }
            catch (NumberFormatException e) {
                return 0;
            }
        }
        else{
            return 0;
        }
    }

    public static long InsertLong(final Object input){
        if (input != null) {
            try {
                return Long.parseLong(input.toString());
            }
            catch (NumberFormatException e) {
                return 0;
            }
        }
        else{
            return 0;
        }
    }
}
