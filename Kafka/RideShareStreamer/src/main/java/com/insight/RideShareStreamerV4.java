package com.insight;

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.InputStream;
import java.util.Iterator;

public class RideShareStreamerV4 extends RideShareStreamerBase {

    public static void main(String[] args) {


        RideShareStreamerV4 rideShareStreamer = new RideShareStreamerV4();

        rideShareStreamer.processStream();
    }

    @Override
    GenericRecord processMessage(GenericRecord val) throws JSONException {
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

        return val;
    }


}