
//////////////////////////////////////////////////////////////////////////
//                                                                      //
//  Source file for class RideShareStreamerV3                           //
//                                                                      //
//  Description: Streamer V3 processes messages corresponding to schema //
//               version 3                                              //
//                                                                      //
//  Author: Sebastian Merkt (@sebmerkt)                                 //
//                                                                      //
//////////////////////////////////////////////////////////////////////////

package com.insight;

import org.apache.avro.generic.GenericRecord;

// Implementation of RideShareStreamerV3 that consumes messages of schema type 3
public class RideShareStreamerV3 extends RideShareStreamerBase {

    public static void main(String[] args) {

        // Initialize class instance and process stream
        RideShareStreamerV3 rideShareStreamer = new RideShareStreamerV3();
        rideShareStreamer.processStream();
    }

    @Override
    GenericRecord processMessage(GenericRecord val) {
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



