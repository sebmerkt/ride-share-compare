
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareConsumerV3                           //
//                                                                      //
//  Description: Consumer V3 consumes messages corresponding to schema  //
//               version 3 and stores the data in the PostGIS DB        //
//               ride_share_A_v3                                        //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

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

// Implementation of RideShareConsumerV3 that consumes messages of schema type 3
public class RideShareConsumerV3 extends RideShareConsumerBase {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        // Initialize class instance
        RideShareConsumerV3 rideShareConsumer = new RideShareConsumerV3();

        // Initialize properties and connect to database
        rideShareConsumer.connect();
        rideShareConsumer.initProperties();

        // Send messages to database
        rideShareConsumer.writeToDB();

    }

    @Override
    void writeToDB(){
        try (final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
                // Consumer will wait for 100ms if no records are found at broker
                final ConsumerRecords<String, GenericRecord> records = consumer.poll(ofMillis(100));
                for (final ConsumerRecord<String, GenericRecord> record : records) {

                    // store intermediate values
                    final String uuid = record.key();
                    final String vendor_name = record.value().get("vendor_name").toString();
                    final String Trip_Pickup_DateTime = InsertString(record.value().get("Trip_Pickup_DateTime"));
                    final String Trip_Dropoff_DateTime = InsertString(record.value().get("Trip_Dropoff_DateTime"));
                    final int Passenger_Count = InsertInt(record.value().get("Passenger_Count"));
                    final double Trip_Distance = InsertDouble(record.value().get("Trip_Distance"));
                    final double Start_Lon = InsertDouble(record.value().get("Start_Lon"));
                    final double Start_Lat = InsertDouble(record.value().get("Start_Lat"));
                    final double End_Lon = InsertDouble(record.value().get("End_Lon"));
                    final double End_Lat = InsertDouble(record.value().get("End_Lat"));
                    final String Payment_Type = record.value().get("Payment_Type").toString();
                    final double Fare_Amt = InsertDouble(record.value().get("Fare_Amt"));
                    final double Tip_Amt = InsertDouble(record.value().get("Tip_Amt"));
                    final double Tolls_Amt = InsertDouble(record.value().get("Tolls_Amt"));
                    final double Total_Amt = InsertDouble(record.value().get("Total_Amt"));
                    final String Process_time = InsertString(record.value().get("Process_time"));

                    // Create SQL statement to insert records an send request
                    Statement stmt = dbConn.createStatement();
                    String sql = "INSERT INTO ride_share_A_v3 " +
                            " ( uuid, vendor_name, Trip_Pickup_DateTime, Trip_Dropoff_DateTime, Passenger_Count, " +
                            "Trip_Distance, Start_Lon, Start_Lat, End_Lon, End_Lat, Payment_Type, Fare_Amt, Tip_Amt, " +
                            "Tolls_Amt, Total_Amt, Process_time, geom_start, geom_end, " +
                                ", ) " +
                            "VALUES ( "+uuid+", '" + vendor_name+"', '"+Trip_Pickup_DateTime+"', '"+Trip_Dropoff_DateTime+"', "+
                                Passenger_Count+", "+Trip_Distance+", "+Start_Lon+", "+Start_Lat+", "+
                                +End_Lon+", "+End_Lat+", '"+Payment_Type+"', "+Fare_Amt+", "+Tip_Amt+", "+Tolls_Amt+", '"
                                +Total_Amt+", "+Process_time+
                                "', 'SRID=4326;POINT("+Start_Lon+" "+Start_Lat+")', 'SRID=4326;POINT("+End_Lon+" "+End_Lat+")' "+")";
                    stmt.executeUpdate(sql);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
