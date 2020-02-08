package com.insight;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.UUID;

import static java.time.Duration.ofMillis;

public class RideShareConsumerV1 extends RideShareConsumerBase {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        RideShareConsumerV1 rideShareConsumer = new RideShareConsumerV1();
        rideShareConsumer.connect();
        rideShareConsumer.initProperties();
        rideShareConsumer.writeToDB();
    }

    @Override
    void writeToDB(){
         try (final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                final ConsumerRecords<String, GenericRecord> records = consumer.poll(ofMillis(10));
                for (final ConsumerRecord<String, GenericRecord> record : records) {

                    final long uuid = Long.valueOf(record.key());
                    final String vendor_name = record.value().get("vendor_name").toString();
                    final String Trip_Pickup_DateTime = InsertString(record.value().get("Trip_Pickup_DateTime"));
                    final String Trip_Dropoff_DateTime = InsertString(record.value().get("Trip_Dropoff_DateTime"));
                    final double Trip_Distance = InsertDouble(record.value().get("Trip_Distance"));
                    final double Start_Lon = InsertDouble(record.value().get("Start_Lon"));
                    final double Start_Lat = InsertDouble(record.value().get("Start_Lat"));
                    final double End_Lon = InsertDouble(record.value().get("End_Lon"));
                    final double End_Lat = InsertDouble(record.value().get("End_Lat"));
                    final double Total_Amt = InsertDouble(record.value().get("Total_Amt"));
                    final String Process_time = InsertString(record.value().get("Process_time"));

                    Statement stmt = dbConn.createStatement();

                    String sql = "INSERT INTO ride_share_A_v1 " +
                            "VALUES ( "+uuid+", '" + vendor_name+"', '"+Trip_Pickup_DateTime+"', '"+Trip_Dropoff_DateTime+
                            "', "+Trip_Distance+", "+Start_Lon+", "+Start_Lat+", "+End_Lon+", "+End_Lat+", "+
                            Total_Amt+", "+Process_time+", 'SRID=4326;POINT("+Start_Lon+" "+Start_Lat+")', 'SRID=4326;POINT("+End_Lon+" "+End_Lat+")' "+")";

                    stmt.executeUpdate(sql);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
