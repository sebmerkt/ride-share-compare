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
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public abstract class RideShareProducerBase <Ride> {

    static String TOPIC = "taxitest9in";

    // construct kafka producer.
    KafkaProducer<String, Ride> producer = null;

    Ride ride = null;

    Ride getRide() {
        return ride;
    }

    void setRide ( final Ride newRide ) {
        ride = newRide;
    }

    KafkaProducer<String, Ride> getProducer() {
        return producer;
    }

    void setProducer ( final KafkaProducer<String, Ride> newProducer ) {
        producer = newProducer;
    }

    public void sendRecords (String[] args) throws IOException {
        int batchNum = 0;
        while (batchNum<args.length) {
            int i = 0;
            System.out.println("Streaming file: "+args[batchNum]);
            BufferedReader br = null;
            String line = "";
            final String cvsSplitBy = ",";

            br = new BufferedReader(new FileReader(args[batchNum]));
            //Read first line
            br.readLine();
            while ((line = br.readLine()) != null) {
                final String[] taxiTrip = line.split(cvsSplitBy, -18);
//                String uniqueID = UUID.randomUUID().toString();

                Date date= new Date();
                Timestamp ts = new Timestamp( date.getTime() );

                Instant instant = Instant.now();
                long timeStampMillis = instant.toEpochMilli();
                System.out.println(timeStampMillis);

                if (i > 0 && !line.contains("NULL")) {
                    buildRecord( taxiTrip );

                    producer.send(new ProducerRecord<String, Ride>(TOPIC, ts.toString(), ride));
                    try{
                        TimeUnit.MILLISECONDS.sleep(10);
                    } catch (final InterruptedException e) {
                        break;
                    }

                }
                i += 1;
            }
            batchNum += 1;
        }
    }

     abstract void buildRecord(final String[] message);
//    abstract void sendRecords ( String[] args, Ride ride, KafkaProducer<String, Ride> producer ) throws IOException;

    public static Properties initProperties() {

        Map<String, String> env = System.getenv();
        String schemaDNS = env.get("SCHEMA_REGISTRY");
        String brokerDNS1 = env.get("BROKER1");
        String brokerDNS2 = env.get("BROKER2");
        String brokerDNS3 = env.get("BROKER3");
        String brokerDNS4 = env.get("BROKER4");


        String schemaUrl = "http://"+schemaDNS+":8081";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                brokerDNS1 + ":9092," + brokerDNS2 + ":9092," + brokerDNS3 + ":9092," + brokerDNS4 + ":9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

        return props;
    }


    public static String InsertString(final String input){
        if (input != null && !input.isEmpty()) {
            return input;
        }
        else{
            return "";
        }
    }

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

}
