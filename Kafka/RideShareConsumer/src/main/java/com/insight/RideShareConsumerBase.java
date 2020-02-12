
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for abstract class RideShareConsumerBase                //
//                                                                      //
//  Description: Provides the base for the Kafka consumers              //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

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

    // output topic
    static final String TOPIC = "taxitest14out";

    // get environment variables
    static Map<String, String> env = System.getenv();

    // Define Kafka and database connection properties
    Properties props = null;
    Connection dbConn = null;

    // Method to write messages to the database
    abstract void writeToDB();

    // Method to initialize Kafka properties
    public void initProperties() {
        String schemaDNS = env.get("SCHEMA_REGISTRY");
        String brokerDNS1 = env.get("BROKER1");
        String brokerDNS2 = env.get("BROKER2");
        String brokerDNS3 = env.get("BROKER3");
        String brokerDNS4 = env.get("BROKER4");

        // Define schema URL
        String schemaUrl = "http://" + schemaDNS + ":8081";

        props = new Properties();
        // Zookeeper and Broker addresses
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                brokerDNS1 + ":9092," + brokerDNS2 + ":9092," + brokerDNS3 + ":9092," + brokerDNS4 + ":9092");
        // consumer group ID
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "rides");
        // Consume from earliest offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // Point to schema registry
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        // Define key and value deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    }

    // Method to connect to the database
    public void connect() {
        String dbServer = env.get("DB_SERVER");
        String dbName = env.get("DB_NAME");
        String url = "jdbc:postgresql://" + dbServer + "/" + dbName;
        String user = env.get("DB_USER");
        String password = env.get("DB_PW");

        try {
            dbConn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

    // Method to safely read strings
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

    // Method to safely insert doubles
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

    // Method to safely insert doubles
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

    // Method to safely insert doubles
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
