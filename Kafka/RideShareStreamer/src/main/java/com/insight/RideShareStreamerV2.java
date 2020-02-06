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

//            System.out.println(val.get("vendor_name"));
//            val.put("vendor_name", String.valueOf(System.currentTimeMillis()));
        System.out.println(val.get("vendor_name"));
        String schema = val.getSchema().toString();
        if (schema.contains("End_Lat")) {
            System.out.println("End_Lat YAY!");
        }
        if (schema.contains("Passenger_Count")) {
            System.out.println("Passenger_Count YAY!");
        }
        Iterator itr = val.getSchema().getFields().iterator();
        while(itr.hasNext()) {
            Schema.Field element = (Schema.Field) itr.next();
            System.out.print(element.defaultVal() + "\n");
            System.out.print(element.getObjectProp(element.name()) + "\n");
        }
        System.out.println(" ");

//            System.out.println(val.get("Payment_Type").getClass().getName());

        return val;
    }


}



