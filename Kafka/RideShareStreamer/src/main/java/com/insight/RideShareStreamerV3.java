package com.insight;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import java.util.Iterator;

public class RideShareStreamerV3 extends RideShareStreamerBase {

    public static void main(String[] args) {

        RideShareStreamerV3 rideShareStreamer = new RideShareStreamerV3();
        rideShareStreamer.processStream();
    }

    @Override
    GenericRecord processMessage(GenericRecord val) {
        // Newer schema has integer codes: 1= Creative Mobile Technologies (CMT), LLC; 2= VeriFone Inc. (VTS)
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





        String schema = val.getSchema().toString();

        Iterator itr = val.getSchema().getFields().iterator();
        while(itr.hasNext()) {
            Schema.Field element = (Schema.Field) itr.next();
            System.out.print(element.defaultVal() + "\n");
            System.out.print(element.getObjectProp(element.name()) + "\n");
        }
        System.out.println(" ");

        return val;
    }
}



