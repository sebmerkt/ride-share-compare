package com.insight;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;


import java.io.*;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RideShareProducer {

    static Map<String, String> env = System.getenv();
    static String schemaDNS = env.get("SCHEMA_REGISTRY");
    static String brokerDNS1 = env.get("BROKER1");
    static String brokerDNS2 = env.get("BROKER2");
    static String brokerDNS3 = env.get("BROKER3");
    static String brokerDNS4 = env.get("BROKER4");


    private static final String schemaUrl = "http://"+schemaDNS+":8081";
    private static final String TOPIC = "taxitest4in";

//    // avro schema avsc file path.
//    private static final String schemaPathBase = "src/main/resources/com/insight/"; ///yellowcab.avsc";

    // subject convention is "<topic-name>-value"
    private static final String subject = TOPIC + "-value";


    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException, RestClientException {

        System.out.println("Starting Producer");

        final Properties props = new Properties();
//        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                brokerDNS1+":9092,"+brokerDNS2+":9092,"+brokerDNS3+":9092,"+brokerDNS4+":9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaUrl);

//        final String schemaPath = schemaPathBase+args[0]+".avsc";
//        final File schemaFile = new File(schemaPath);
//        final Schema avroSchema = new Schema.Parser().parse(schemaFile);


//        final CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(schemaUrl, 20);

//        client.register(subject, avroSchema);

        // construct kafka producer.
        final Producer<String, Ride> producer = new KafkaProducer<>(props);// message key.

//        final String[] csvFile = {"/home/ubuntu/yellow_tripdata_2009-01_V1.csv",
//                "/home/ubuntu/yellow_tripdata_2009-01_V2.csv"};
//          final String[] csvFile = {"/home/ubuntu/nyc-taxi-rideshare/trip_data/yellow_tripdata_2009-01.csv",
//                  "/home/ubuntu/nyc-taxi-rideshare/trip_data/yellow_tripdata_2015-01.csv"};

        Ride1 ride = new Ride1();

        int batchNum = 0;
        while (batchNum<args.length) {
            int i = 0;
            System.out.println("Streaming file: "+args[batchNum]);
            BufferedReader br = null;
            String line = "";
            final String cvsSplitBy = ",";

            br = new BufferedReader(new FileReader(args[batchNum]));
            br.readLine();  //Read first line
            while ((line = br.readLine()) != null) {
                final String[] taxiTrip = line.split(cvsSplitBy, -18);
                String uniqueID = UUID.randomUUID().toString();

                if (i > 0 && !line.contains("NULL")) {
                    buildRecord(ride, taxiTrip);// send avro message to the topic page-view-event.

                    producer.send(new ProducerRecord<String, Ride>(TOPIC, uniqueID, ride));
                    try{
                        TimeUnit.SECONDS .sleep(1);
                    } catch (final InterruptedException e) {
                        break;
                    }

                }
                i += 1;
            }
            batchNum += 1;
        }
        producer.flush();
        producer.close();
    }


    public static void buildRecord( final Ride ride, final String[] transaction) {
        ride.setVendorName( InsertString(transaction[0]) );
        ride.setTripPickupDateTime( InsertString(transaction[1]) );
        ride.setTripDropoffDateTime( InsertString(transaction[2]) );
        ride.setTripDistance( InsertDouble(transaction[3]) );
        ride.setStartLon( InsertDouble(transaction[4]) );
        ride.setStartLat( InsertDouble(transaction[5]) );
        ride.setEndLon( InsertDouble(transaction[6]) );
        ride.setEndLat( InsertDouble(transaction[7]) );
        ride.setTotalAmt( InsertDouble(transaction[8]) );

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


}

