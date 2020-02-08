package com.insight;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;

import static java.time.Duration.ofMillis;

public class RideShareConsumerV5 extends RideShareConsumerBase {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        RideShareConsumerV5 rideShareConsumer = new RideShareConsumerV5();
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

                    final String vendor_name = record.value().get("vendor_name").toString();
                    final String Trip_Pickup_DateTime = InsertString(record.value().get("Trip_Pickup_DateTime"));
                    final String Trip_Dropoff_DateTime = InsertString(record.value().get("Trip_Dropoff_DateTime"));
                    final int Passenger_Count = InsertInt(record.value().get("Passenger_Count"));
                    final double Trip_Distance = InsertDouble(record.value().get("Trip_Distance"));
                    final double Start_Lon = InsertDouble(record.value().get("Start_Lon"));
                    final double Start_Lat = InsertDouble(record.value().get("Start_Lat"));
//                    final long PULocationID = InsertLong(record.value().get("PULocationID"));
                    final double Rate_Code = InsertDouble(record.value().get("Rate_Code"));
                    final double store_and_forward = InsertDouble(record.value().get("store_and_forward"));
                    final double End_Lon = InsertDouble(record.value().get("End_Lon"));
                    final double End_Lat = InsertDouble(record.value().get("End_Lat"));
//                    final long DOLocationID = InsertLong(record.value().get("DOLocationID"));
                    final String Payment_Type = record.value().get("Payment_Type").toString();
                    final double Fare_Amt = InsertDouble(record.value().get("Fare_Amt"));
                    final double surcharge = InsertDouble(record.value().get("surcharge"));
                    final double mta_tax = InsertDouble(record.value().get("mta_tax"));
                    final double Tip_Amt = InsertDouble(record.value().get("Tip_Amt"));
                    final double Tolls_Amt = InsertDouble(record.value().get("Tolls_Amt"));
                    final double Total_Amt = InsertDouble(record.value().get("Total_Amt"));

                    Statement stmt = dbConn.createStatement();

                    String sql = "INSERT INTO ride_share_A_v4 " +
                            "VALUES ('" + vendor_name+"', '"+Trip_Pickup_DateTime+"', '"+Trip_Dropoff_DateTime+"', "+
                            Passenger_Count+", "+Trip_Distance+", "+Start_Lon+", "+Start_Lat+", "+Rate_Code+", "+
                            store_and_forward+", "+End_Lon+", "+End_Lat+", '"+Payment_Type+"', "+
                            Fare_Amt+", "+surcharge+", "+mta_tax+", "+Tip_Amt+", "+Tolls_Amt+", "+Total_Amt+
                            ", 'SRID=4326;POINT("+Start_Lon+" "+Start_Lat+")', 'SRID=4326;POINT("+End_Lon+" "+End_Lat+")' "+")";

                    stmt.executeUpdate(sql);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}