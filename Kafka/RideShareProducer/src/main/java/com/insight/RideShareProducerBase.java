
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for abstract class RideShareProducerBase                //
//                                                                      //
//  Description: Provides the base for the Kafka producers              //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

package com.insight;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

// Definition of the producer base class producing messages of the generic schema class Ride
public abstract class RideShareProducerBase <Ride> {

    // Input topic
    static String TOPIC = "ride-share-input-test5";

    // Define kafka producer and basic accessors
    KafkaProducer<String, Ride> producer = null;

    KafkaProducer<String, Ride> getProducer() {
        return producer;
    }

    void setProducer ( final KafkaProducer<String, Ride> newProducer ) {
        producer = newProducer;
    }

    // Define the schema class and basic accessors
    Ride ride = null;

    Ride getRide() {
        return ride;
    }

    void setRide ( final Ride newRide ) {
        ride = newRide;
    }

    // Method for sending messages
    public void sendRecords (String[] args) throws IOException {

        // Variable for keeping track of the input files
        int batchNum = 0;

        // Loop through the input files. Input files are given through command line arguments
        while (batchNum<args.length) {
            // Keep track of lines in input file
            int i = 0;

            System.out.println("Streaming file: "+args[batchNum]);

            // Initialize buffer reader
            BufferedReader br = null;

            // Initialize string to store input line
            String line = "";

            // Separator in CSV file
            final String cvsSplitBy = ",";

            // Open file
            br = new BufferedReader(new FileReader(args[batchNum]));

            //Read first line (Don't send headers as a message)
            br.readLine();

            // Loop through the lines in the file
            while ((line = br.readLine()) != null) {

                // Split each line into fields
                final String[] taxiTrip = line.split(cvsSplitBy, 0);

                // Generate a unique ID as key
                String uniqueID = UUID.randomUUID().toString();

                // Check if line is a valid input
                if (i > 0 && !line.contains("NULL")) {

                    // Build the record to be sent
                    buildRecord( taxiTrip );

                    // Send the message to the Kafka input topic
                    producer.send(new ProducerRecord<String, Ride>(TOPIC, uniqueID, ride));
                    try{
                        // Control the frequency of messages to be sent
                        TimeUnit.MILLISECONDS.sleep(100);
                    } catch (final InterruptedException e) {
                        break;
                    }

                }
                i += 1;
            }
            batchNum += 1;
        }
    }

    // Abstract definition of the build record class. The implementation depends on the used schema.
    abstract void buildRecord(final String[] message);

    // Initialize Kafka producer properties
    public static Properties initProperties() {

        // Read out the Kafka specific environment variables
        Map<String, String> env = System.getenv();
        String schemaDNS = env.get("SCHEMA_REGISTRY");
        String brokerDNS1 = env.get("BROKER1");
        String brokerDNS2 = env.get("BROKER2");
        String brokerDNS3 = env.get("BROKER3");
        String brokerDNS4 = env.get("BROKER4");

        // Address of the schema registry
        String schemaUrl = "http://"+schemaDNS+":8081";

        // Construct the properties
        Properties props = new Properties();

        // Addresses of the Kafka Zookeeper and Brokers
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                brokerDNS1 + ":9092," + brokerDNS2 + ":9092," + brokerDNS3 + ":9092," + brokerDNS4 + ":9092");
        // All messages require acknowledgement
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        // Do not retry
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        // Define key and value serializers
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        // Point to schema registry
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

        return props;
    }

    // Method to check string validity and catch any exceptions
    public static String InsertString(final String input){
        if (input != null && !input.isEmpty()) {
            return input;
        }
        else{
            return "";
        }
    }

    // Method to convert long to string and catch any exceptions
    public static long InsertLong(final String input){
        if (input != null && !input.isEmpty()) {
            try {
                return Long.parseLong(input);
            }
            catch (NumberFormatException e) {
                return 0;
            }
        }
        else{
            return 0;
        }
    }

    // Method to convert double to string and catch any exceptions
    public static double InsertDouble(final String input){
        if (input != null && !input.isEmpty()) {
            try {
                return Double.parseDouble(input);
            }
            catch (NumberFormatException e) {
                return 0.0;
            }
        }
        else{
            return 0.0;
        }
    }

    // Method to convert int to string and catch any exceptions
    public static int InsertInt(final String input){
        if (input != null && !input.isEmpty()) {
            try {
                return Integer.parseInt(input);
            }
            catch (NumberFormatException e) {
                return 0;
            }
        }
        else{
            return 0;
        }
    }

    public String AssignProvider() {
        String provider = "";
        return provider;
    }
}
