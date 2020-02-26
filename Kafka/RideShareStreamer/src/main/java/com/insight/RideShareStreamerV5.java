
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareStreamerV5                           //
//                                                                      //
//  Description: Streamer V5 processes messages corresponding to schema //
//               version 5                                              //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

package com.insight;

import org.apache.avro.generic.GenericRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

// Implementation of RideShareStreamerV5 that consumes messages of schema type 5
public class RideShareStreamerV5 extends RideShareStreamerBase {

    public static void main(String[] args) {

        // Initialize class instance and process stream
        RideShareStreamerV5 rideShareStreamer = new RideShareStreamerV5();
        rideShareStreamer.processStream();
    }

    @Override
    GenericRecord processMessage(GenericRecord val) throws JSONException, IOException {
        // Newer schema have integer codes: 1= Creative Mobile Technologies (CMT), LLC; 2= VeriFone Inc. (VTS)
        if ( val.get("vendor_name") == "1" ) {
            val.put("vendor_name", "CMT");
        }
        else if ( val.get("vendor_name") == "2" ) {
            val.put("vendor_name", "VTS");
        }


        //Newer schema integer codes: 1= Credit card, 2= Cash, 3= No charge, 4= Dispute, 5= Unknown, 6= Voided trip
        if ( val.get("Payment_Type") == "1" ) {
            val.put("Payment_Type", "Credit");
        }
        else if ( val.get("Payment_Type") == "2" ) {
            val.put("Payment_Type", "CASH");
        }
        else if ( val.get("Payment_Type") == "3" ) {
            val.put("Payment_Type", "No Charge");
        }
        else if ( val.get("Payment_Type") == "4" ) {
            val.put("Payment_Type", "Dispute");
        }
        else if ( val.get("Payment_Type") == "5" ) {
            val.put("Payment_Type", "Unknown");
        }
        else if ( val.get("Payment_Type") == "5" ) {
            val.put("Payment_Type", "Voided trip");
        }

        // open file to translate between Location IDs and geographical coordinates
        final InputStream inputStream = getClass().getResourceAsStream("taxi_zones.json");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
        StringBuilder stringBuilder = new StringBuilder();
        String inputStr;
        while ((inputStr = bufferedReader.readLine()) != null)
            stringBuilder.append(inputStr);
        JSONObject obj = new JSONObject(stringBuilder.toString());

        if ( val.get("PULocationID").toString()!="") {
            // Get Pickup Location ID from the message
            String PULocID = String.valueOf(Long.parseLong(val.get("PULocationID").toString()) - 1);
            // Look up coordinates of this Location ID
            if (Long.parseLong(PULocID) <= 262) {
                val.put("Start_Lon", obj.getJSONObject("X").get(PULocID));
                val.put("Start_Lat", obj.getJSONObject("Y").get(PULocID));
            }
        }

        if ( val.get("DOLocationID").toString()!="") {
            // Get Dropoff Location ID from the message
            String DOLocID = String.valueOf(Long.parseLong(val.get("DOLocationID").toString()) - 1);
            // Look up coordinates of this Location ID
            if (Long.parseLong(DOLocID) <= 262) {
                val.put("End_Lon", obj.getJSONObject("X").get(DOLocID));
                val.put("End_Lat", obj.getJSONObject("Y").get(DOLocID));
            }
        }

        return val;
    }


}