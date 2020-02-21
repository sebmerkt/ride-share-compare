
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareConsumerV4                           //
//                                                                      //
//  Description: Consumer V4 consumes messages corresponding to schema  //
//               version 4 and stores the data in the PostGIS DB        //
//               ride_share_A_v4                                        //
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

import static java.time.Duration.ofMillis;

// Implementation of RideShareConsumerV4 that consumes messages of schema type 4
public class BikeShareConsumerV1 extends RideShareConsumerBase {

    @SuppressWarnings("InfiniteLoopStatement")
    public static void main(final String[] args) {

        // Initialize class instance
        BikeShareConsumerV1 rideShareConsumer = new BikeShareConsumerV1();

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
                    final String uuid = InsertString(record.key());
                    final String vendor_name = InsertString(record.value().get("vendor_name"));
                    final String Trip_Pickup_DateTime = InsertString(record.value().get("Trip_Pickup_DateTime"));
                    final String Trip_Dropoff_DateTime = InsertString(record.value().get("Trip_Dropoff_DateTime"));
                    final double Start_Lon = InsertDouble(record.value().get("Start_Lon"));
                    final double Start_Lat = InsertDouble(record.value().get("Start_Lat"));
                    final double End_Lon = InsertDouble(record.value().get("End_Lon"));
                    final double End_Lat = InsertDouble(record.value().get("End_Lat"));
                    final String Process_time = InsertString(record.value().get("Process_time"));
                    final long Trip_Duration = InsertLong(record.value().get("tripduration"));
                    final long Start_Station_Id = InsertLong(record.value().get("start_station_id"));
                    final String Start_Station_Name = InsertString(record.value().get("start_station_name"));
                    final long End_Station_Id = InsertLong(record.value().get("end_station_id"));
                    final String End_Station_Name = InsertString(record.value().get("end_station_name"));
                    final long Bike_Id = InsertLong(record.value().get("bikeid"));
                    final String User_Type = InsertString(record.value().get("usertype"));
                    final long Birth_Year = InsertLong(record.value().get("birth_year"));
                    final long Gender = InsertLong(record.value().get("gender"));

                    // Create SQL statement to insert records an send request
                    Statement stmt = dbConn.createStatement();
                    String sql = "INSERT INTO ride_share_data " +
                                " ( uuid, vendor_name, Trip_Pickup_DateTime, Trip_Dropoff_DateTime, Trip_Duration, " +
                                "Start_Station_Id, Start_Lon, Start_Lat, Start_Station_Name, End_Station_Id, End_Lon, End_Lat, " +
                                "End_Station_Name, Bike_Id, User_Type, Birth_Year, Gender, " +
                            "Process_time, geom_start, geom_end ) " +
                            "VALUES ( '"+uuid+"', '" + vendor_name+"', '"+Trip_Pickup_DateTime+"', '"+Trip_Dropoff_DateTime+"', "+
                                Trip_Duration+", "+Start_Station_Id+", "+Start_Lon+", "+Start_Lat+", '"+Start_Station_Name+"', "+
                                End_Station_Id+", "+End_Lon+", "+End_Lat+", '"+End_Station_Name+"', "+
                                Bike_Id+", "+User_Type+", "+Birth_Year+", "+Gender+", '"+Process_time+
                                "', 'SRID=4326;POINT("+Start_Lon+" "+Start_Lat+")', 'SRID=4326;POINT("+End_Lon+" "+End_Lat+")' )";
                    stmt.executeUpdate(sql);
                }
            }

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}