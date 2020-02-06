package com.insight;

import org.apache.avro.generic.GenericRecord;

public class RideShareStreamerV1 extends RideShareStreamerBase {
    public static void main(String[] args) {

        RideShareStreamerV1 rideShareStreamer = new RideShareStreamerV1();
        rideShareStreamer.processStream();
    }

    @Override
    GenericRecord processMessage(GenericRecord val) {
        return val;
    }

}
