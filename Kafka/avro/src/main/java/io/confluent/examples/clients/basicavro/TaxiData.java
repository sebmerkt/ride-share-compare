package io.confluent.examples.clients.basicavro;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.kafka.clients.producer.Producer;

import java.io.BufferedReader;

public class TaxiData {
    String key = "";
    S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
    BufferedReader br = null;
    String line = "";
    Double PULID = 0.0;
    Double DOLID = 0.0;
    Double SRflag = 0.0;

    String getKey(){
        return key;
    }
    void setKey(final String newKey){
        key = newKey;
    }

    S3Object getFullObject(){
        return fullObject;
    }
    void setFullObject(final S3Object NewFullObject){
        fullObject = NewFullObject;
    }

    S3Object getObjectPortion(){
        return objectPortion;
    }
    void setObjectPortion(final S3Object NewObjectPortion){
        objectPortion = NewObjectPortion;
    }

    S3Object getHeaderOverrideObject(){
        return headerOverrideObject;
    }
    void setHeaderOverrideObject(final S3Object NewHeaderOverrideObject){
        headerOverrideObject = NewHeaderOverrideObject;
    }

    BufferedReader getBr(){
        return br;
    }
    void setBr(final BufferedReader newBr){
        br = newBr;
    }

    String getLine(){
        return line;
    }
    void setLine(final String newLine){
        line = newLine;
    }

    Double getPULID(){
        return PULID;
    }
    void setPULID(final Double NewPULID){
        PULID = NewPULID;
    }

    Double getDOLID(){
        return DOLID;
    }
    void setDOLID(final Double NewDOLID){
        PULID = NewDOLID;
    }

    Double getSRflag(){
        return SRflag;
    }
    void setSRflag(final Double NewSRflag){
        SRflag = NewSRflag;
    }
}
