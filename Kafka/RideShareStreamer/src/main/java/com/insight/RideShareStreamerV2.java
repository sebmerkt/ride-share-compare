package com.insight;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;

public class RideShareStreamerV2 extends RideShareStreamerBase {

    public static void main(String[] args) {

        RideShareStreamerV2 rideShareStreamer = new RideShareStreamerV2();
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


        return val;
    }


}



