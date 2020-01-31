package io.confluent.examples.clients.basicavro;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.errors.SerializationException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class ProducerExample {



    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        final TaxiData taxiData = new TaxiData();

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        final String bucketName = "nyc-taxi-rideshare";
        try{
            final AmazonS3 s3client = AmazonS3ClientBuilder
                    .standard()
                    .withRegion(Regions.US_WEST_2)
                    .build();

            final ObjectListing objectListing = s3client.listObjects(bucketName);
            for(final S3ObjectSummary os : objectListing.getObjectSummaries()) {
//                System.out.println(os.getKey());
                if (os.getKey().contains("fhv_tripdata_2019-01")){
                    taxiData.setKey(os.getKey());
                }
            }

            // Get an object and print its contents.
            taxiData.setFullObject(s3client.getObject(new GetObjectRequest(bucketName, taxiData.getKey())));

            final Producer<String, String> producer = new KafkaProducer<>(props);
            int batchNum = 0;

            while (true) {
                int i = 0;
                System.out.println("Producing batch: " + batchNum);
                batchNum += 1;

                final String csvFile = "/home/ubuntu/fhv_tripdata_2019-01.csv";
                String line = "";
                final String cvsSplitBy = ",";

                taxiData.setBr(new BufferedReader(new FileReader(csvFile)));
                taxiData.getBr().readLine();  //Read first line
                while ((line = taxiData.getBr().readLine()) != null) {
//                        System.out.println(line);
                    final String[] transaction = line.split(cvsSplitBy, -18);

                    if (i > 0 && !line.contains("NULL") ) {
                        try {
                            //get data from the columns
                            final String base = transaction[0];
                            final String pickuptimeIn = transaction[1];
                            final String dropofftimeIn = transaction[2];

                            if (transaction[3] != null && !transaction[3].isEmpty()) {
                                taxiData.setPULID(Double.valueOf(transaction[3]));
                            }
                            if (transaction[4] != null && !transaction[4].isEmpty()) {
                                taxiData.setDOLID(Double.valueOf(transaction[4]));
                            }
                            if (transaction[5] != null && !transaction[5].isEmpty()) {
                                taxiData.setSRflag(Double.valueOf(transaction[5]));
                            }



                            //tranform time to localdatetime
                            final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss");//2019-02-01 00:39:56
                            final LocalDateTime pickuptime = LocalDateTime.parse(pickuptimeIn, formatter);
                            final LocalDateTime dropofftime = LocalDateTime.parse(dropofftimeIn, formatter);

                            producer.send(newTransaction(base, pickuptime, dropofftime, taxiData.getPULID(), taxiData.getDOLID(), taxiData.getSRflag()));

                            TimeUnit.SECONDS.sleep(1);
                            //                            Thread.sleep(5);

                        } catch (final InterruptedException e) {
                            break;
                        }
                    }
                    i += 1;
                }
            }



        } catch (final AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (final SdkClientException | IOException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } finally {
            // To ensure that the network connection doesn't remain open, close any open input streams.
            if (taxiData.getFullObject() != null) {
                taxiData.getFullObject().close();
            }
            if (taxiData.getObjectPortion() != null) {
                taxiData.getObjectPortion().close();
            }
            if (taxiData.getHeaderOverrideObject() != null) {
                taxiData.getHeaderOverrideObject().close();
            }
        }



    }


    public static ProducerRecord<String, String> newTransaction(final String base, final LocalDateTime pickuptime,
                                                                final LocalDateTime dropofftime, final Double PULID,
                                                                final Double DOLID, final Double SRflag) {
        // creates an empty json {}
        final ObjectNode transaction = JsonNodeFactory.instance.objectNode();

        final UUID uuid = UUID.randomUUID();
        final String uniqueID = uuid.toString();
        transaction.put("ID", uniqueID);
        transaction.put("dispatching_base_num", base);
        transaction.put("pickup_datetime", pickuptime.toString());
        transaction.put("dropoff_datetime", dropofftime.toString());
        transaction.put("PULocationID", PULID);
        transaction.put("DOLocationID", DOLID);
        transaction.put("SR_Flag", SRflag);

        return new ProducerRecord<>("taxiinput", uniqueID, transaction.toString());
    }
}

