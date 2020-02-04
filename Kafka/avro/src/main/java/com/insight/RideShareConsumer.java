package com.insight;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofMillis;

public class RideShareConsumer {

    private static final String TOPIC = "taxitest4out";

    static Map<String, String> env = System.getenv();
    static String schemaDNS = env.get("SCHEMA_REGISTRY");
    static String brokerDNS1 = env.get("BROKER1");
    static String brokerDNS2 = env.get("BROKER2");
    static String brokerDNS3 = env.get("BROKER3");
    static String brokerDNS4 = env.get("BROKER4");

    static String dbServer = env.get("DB_SERVER");
    static String dbName = env.get("DB_NAME");

    private final static String url = "jdbc:postgresql://"+dbServer+"/"+dbName;
    private final static String user = env.get("DB_USER");
    private final static String password = env.get("DB_PW");

    private static final String schemaUrl = "http://"+schemaDNS+":8081";

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {


        RideShareConsumer app = new RideShareConsumer();
        Connection dbConn = app.connect();

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                brokerDNS1+":9092,"+brokerDNS2+":9092,"+brokerDNS3+":9092,"+brokerDNS4+":9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-rides");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "5000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
//        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        try (final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                final ConsumerRecords<String, GenericRecord> records = consumer.poll(ofMillis(10));
                for (final ConsumerRecord<String, GenericRecord> record : records) {
                    final String key = record.key();
                    final GenericRecord value = record.value();

                    final String vendor_name = record.value().get("vendor_name").toString();
                    final String Trip_Pickup_DateTime = record.value().get("Trip_Pickup_DateTime").toString();
                    final String Trip_Dropoff_DateTime = record.value().get("Trip_Dropoff_DateTime").toString();
//                    final long Passenger_Count = (long) record.value().get("Passenger_Count");
                    final double Trip_Distance = (double) record.value().get("Trip_Distance");
                    final double Start_Lon = (double) record.value().get("Start_Lon");
                    final double Start_Lat = (double) record.value().get("Start_Lat");
//                    final double Rate_Code = (double) record.value().get("Rate_Code");
//                    final double store_and_forward = (double) record.value().get("store_and_forward");
                    final double End_Lon = (double) record.value().get("End_Lon");
                    final double End_Lat = (double) record.value().get("End_Lat");
//                    final String Payment_Type = record.value().get("Payment_Type").toString();
//                    final double Fare_Amt = (double) record.value().get("Fare_Amt");
//                    final double surcharge = (double) record.value().get("surcharge");
//                    final double mta_tax = (double) record.value().get("mta_tax");
//                    final double Tip_Amt = (double) record.value().get("Tip_Amt");
//                    final double Tolls_Amt = (double) record.value().get("Tolls_Amt");
                    final double Total_Amt = (double) record.value().get("Total_Amt");

                    Statement stmt = dbConn.createStatement();


//                    String sql = "INSERT INTO yellowcabsV1 " +
//                            "VALUES ('" + vendor_name+"', '"+Trip_Pickup_DateTime+"', '"+Trip_Dropoff_DateTime+"', "+
//                            Passenger_Count+", "+Trip_Distance+", "+Start_Lon+", "+Start_Lat+", "+Rate_Code+", "+
//                            store_and_forward+", "+End_Lon+", "+End_Lat+", '"+Payment_Type+"', "+
//                            Fare_Amt+", "+surcharge+", "+mta_tax+", "+Tip_Amt+", "+Tolls_Amt+", "+Total_Amt+
//                            ", 'SRID=4326;POINT("+Start_Lon+" "+Start_Lat+")', 'SRID=4326;POINT("+End_Lon+" "+End_Lat+")' "+")";


                    String sql = "INSERT INTO yellowcabsV1 " +
                            "VALUES ('" + vendor_name+"', '"+Trip_Pickup_DateTime+"', '"+Trip_Dropoff_DateTime+
                            ", "+Trip_Distance+", "+Start_Lon+", "+Start_Lat+", "+End_Lon+", "+End_Lat+"', "+
                            ", "+Total_Amt+", 'SRID=4326;POINT("+Start_Lon+" "+Start_Lat+")', 'SRID=4326;POINT("+End_Lon+" "+End_Lat+")' "+")";
                    System.out.println(sql);
                    stmt.executeUpdate(sql);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }

        return conn;
    }
}
