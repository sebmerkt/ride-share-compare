package com.insight;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Iterator;
import java.util.Properties;

public class RideShareProducer_TEST {

    private static final String TOPIC = "taxitest4in";

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        try (KafkaProducer<String, Ride1> producer = new KafkaProducer<String, Ride1>(props)) {

            System.out.println(Ride1.getClassSchema());
            for (long i = 0; i < 10; i++) {

                final Ride1 ride = new Ride1();
//                final Ride ride = new Ride(vendor_name, Trip_Pickup_DateTime, Trip_Dropoff_DateTime, Trip_Distance, Start_Lon, Start_Lat, End_Lon, End_Lat, Total_Amt)

                ride.setVendorName("Vendor "+String.valueOf(i));

                System.out.println(ride.getSchema().getFields());
                Iterator itr = ride.getSchema().getFields().iterator();
                while(itr.hasNext()) {
                    Schema.Field element = (Schema.Field) itr.next();
                    System.out.print(element.name() + "\n ");
                }
                final ProducerRecord<String, Ride1> record = new ProducerRecord<String, Ride1>(TOPIC, "test",
                        ride);
                producer.send(record);
                Thread.sleep(1000L);
            }


            producer.flush();
            System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);

        } catch (final SerializationException e) {
            e.printStackTrace();
        } catch (final InterruptedException e) {
            e.printStackTrace();
        }

    }
}

