package com.insight;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class RideShareProducerV1 extends RideShareProducerBase {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) throws IOException {

        final Properties props = initProperties();

        // construct kafka producer.
        final KafkaProducer<String, Ride1> producer = new KafkaProducer<>(props);

        Ride1 ride = new Ride1();

        sendRecords(args, ride, producer);
        producer.flush();
        producer.close();
    }

    public static void sendRecords ( String[] args, Ride1 ride, KafkaProducer<String, Ride1> producer ) throws IOException {
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
                String uniqueID = UUID.randomUUID().toString();

                if (i > 0 && !line.contains("NULL")) {
                    buildRecord(ride, taxiTrip);

                    producer.send(new ProducerRecord<String, Ride1>(TOPIC, uniqueID, ride));
                    try{
                        TimeUnit.SECONDS.sleep(1);
                    } catch (final InterruptedException e) {
                        break;
                    }

                }
                i += 1;
            }
            batchNum += 1;
        }
    }


    static void buildRecord(final Ride1 ride, final String[] transaction) {
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


}

