/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package KafkaConsumer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;

import java.io.PrintWriter;
import java.util.Properties;

/**
 * Example illustrating iterations in Flink streaming.
 * <p> The program sums up random numbers and counts additions
 * it performs to reach a specific threshold in an iterative streaming fashion. </p>
 *
 * <p>
 * This example shows how to use:
 * <ul>
 *   <li>streaming iterations,
 *   <li>buffer timeout to enhance latency,
 *   <li>directed outputs.
 * </ul>
 * </p>
 */
public class WordsCapitalizer {// implements MapFunction<String, String> {
//    @Override
//    public String map(String s) {
//        return s.toUpperCase();
//    }
    private static final int BOUND = 100;

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);


        // obtain execution environment and set setBufferTimeout to 1 to enable
        // continuous flushing of the output buffers (lowest latency)
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment()
                .setBufferTimeout(1);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);


        Properties properties = new Properties();
        properties.setProperty("zookeeper.connect", "ec2-44-231-244-3.us-west-2.compute.amazonaws.com:2181");
        properties.setProperty("bootstrap.servers", "ec2-44-231-244-3.us-west-2.compute.amazonaws.com:9092");
        properties.setProperty("group.id", "my-group");                 // Consumer group ID
        //properties.setProperty("auto.offset.reset", "earliest");       // Always read topic from start

//        FlinkKafkaConsumer010<String> kafkaConsumer = createStringConsumerForTopic(
//                "taxiinput", "ec2-44-231-244-3.us-west-2.compute.amazonaws.com:9092", "my-group");

        FlinkKafkaConsumer010<String> kafkaConsumer = createStringConsumerForTopic(
                "taxiinput", "ec2-44-231-244-3.us-west-2.compute.amazonaws.com:9092", "my-group");


        // convert kafka stream to data stream
        DataStream<String> rawInputStream = env.addSource(kafkaConsumer);


        PrintWriter out = new PrintWriter("filename.txt");
        out.println("START");
        out.println(params.toString());

        out.println(rawInputStream.toString());
        out.close();

        FlinkKafkaProducer010<String> flinkKafkaProducer = createStringProducer(
                "taxioutput", "ec2-44-231-244-3.us-west-2.compute.amazonaws.com:9092");
//        FlinkKafkaProducer010<String> flinkKafkaProducer= new FlinkKafkaProducer010<String>(
//                "localhost:9092",
//                "taxioutput",
//                new SimpleStringSchema() );

        DataStream<String> transformStream = rawInputStream.map(String::toUpperCase);
        transformStream.addSink(flinkKafkaProducer);


//        rawInputStream.map(String::toUpperCase).writeAsText("flink_out_new.txt");
//        rawInputStream.map(String::toUpperCase).print();


        // execute the program
        env.execute("Streaming Iteration Example");
    }

    public static FlinkKafkaConsumer010<String> createStringConsumerForTopic(
            String topic, String kafkaAddress, String kafkaGroup ) {

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", kafkaAddress);
        props.setProperty("group.id",kafkaGroup);

        return new FlinkKafkaConsumer010<>(
                topic, new SimpleStringSchema(), props);
    }
//
    public static FlinkKafkaProducer010<String> createStringProducer(
            String topic, String kafkaAddress){

        return new FlinkKafkaProducer010<>(kafkaAddress,
                topic, new SimpleStringSchema());
    }


}




}