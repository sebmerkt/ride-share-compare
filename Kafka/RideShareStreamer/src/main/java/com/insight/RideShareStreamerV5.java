package com.insight;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.Iterator;

public class RideShareStreamerV5 extends RideShareStreamerBase {

    public static void main(String[] args) {


        RideShareStreamerV5 rideShareStreamer = new RideShareStreamerV5();

        rideShareStreamer.processStream();
    }

    @Override
    GenericRecord processMessage(GenericRecord val) throws JSONException, IOException {
        // Newer schema have integer codes: 1= Creative Mobile Technologies (CMT), LLC; 2= VeriFone Inc. (VTS)
        if ( val.get("vendor_name") == "1" ) {
            val.put("vendor_name", String.valueOf("CMT"));
        }
        else if ( val.get("vendor_name") == "2" ) {
            val.put("vendor_name", String.valueOf("VTS"));
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

        final InputStream resourceAsStream = getClass().getResourceAsStream("taxi_zones.json");
//        val.get("PULocationID");


        BufferedReader streamReader = new BufferedReader(new InputStreamReader(resourceAsStream, "UTF-8"));
        StringBuilder responseStrBuilder = new StringBuilder();

        String inputStr;
        while ((inputStr = streamReader.readLine()) != null)
            responseStrBuilder.append(inputStr);
        JSONObject obj = new JSONObject(responseStrBuilder.toString());

        String PULocID = val.get("PULocationID").toString();
        if ( Integer.parseInt(PULocID)<=263 ){
            val.put("Start_Lon",obj.getJSONObject("X").get(PULocID));
            val.put("Start_Lat",obj.getJSONObject("Y").get(PULocID));
        }

        String DOLocID = val.get("DOLocationID").toString();
        if ( Integer.parseInt(DOLocID)<=263 ){
            val.put("End_Lon", obj.getJSONObject("X").get(DOLocID));
            val.put("End_Lat", obj.getJSONObject("Y").get(DOLocID));
        }

        return val;
    }


}
//
// {"name": "PULocationID", "type": ["long", "null"], "default": 0},
//          {"name": "Rate_Code", "type": ["double", "null"], "default": 0.0, "alias": "RatecodeID"},
//          {"name": "store_and_forward", "type": ["double", "null"], "default": 0.0, "alias": "store_and_fwd_flag"},
//          {"name": "DOLocationID", "type": ["long", "null"], "default": 0},
//          {"name": "Payment_Type", "type": ["string", "null"], "default": "", "alias": "payment_type"},
//          {"name": "Fare_Amt", "type": ["double", "null"], "default": 0.0, "alias": "fare_amount"},
//          {"name": "extra", "type": ["double", "null"], "default": 0.0},
//          {"name": "surcharge", "type": ["double", "null"], "default": 0.0, "alias": "improvement_surcharge"},
//          {"name": "mta_tax", "type": ["double", "null"], "default": 0.0},
//          {"name": "Tip_Amt", "type": ["double", "null"], "default": 0.0, "alias": "tip_amount"},
//          {"name": "Tolls_Amt", "type": ["double", "null"], "default": 0.0, "alias": "tolls_amount"},
//          {"name": "Total_Amt", "type": ["double", "null"], "default": 0.0, "alias": "total_amount"}
//          ]