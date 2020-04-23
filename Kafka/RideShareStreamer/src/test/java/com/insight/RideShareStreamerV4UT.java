package com.insight;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.json.JSONException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RideShareStreamerV4UT {

    private final String topicIn = "topic-in";
    private final String topicOut = "topic-out";
    private final String schemaUrl = "http://localhost:8081";
    // http://localhost:8081/subjects/topic-in-value/versions/latest
    // only for TopicNameStrategy
    private final String mockedUrl = schemaUrl + "/subjects/" + topicIn + "-value/versions/latest";
    private TopologyTestDriver testDriver;
    private MockSchemaRegistryClient schemaRegistryClient;
    private Properties properties;


    // A mocked schema registry for our serdes to use
    private static final String SCHEMA_REGISTRY_SCOPE = RideShareStreamerV4UT.class.getName();
    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;

    @Before
    public void start() {
        properties = new Properties();
        properties.put(StreamsConfig.CLIENT_ID_CONFIG, "client-id-test-1");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-id-test-1");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9922");
        properties.put(
                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        properties.put(
                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
//        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        schemaRegistryClient = new MockSchemaRegistryClient();
    }


    @After
    public void tearDown() {
        Optional.ofNullable(testDriver).ifPresent(TopologyTestDriver::close);
        testDriver = null;
        properties = null;
    }


    @Test
    public void test_streamProcessing() throws IOException, RestClientException {

        /** Arrange **/
        // register schema in mock schema-registry -> not necessary
        // schemaRegistryClient.register(
        //    new TopicNameStrategy().subjectName(topicIn, false, Person.SCHEMA$), Person.SCHEMA$);
        // create serde with config to be able to connect to mock schema registry
        // https://github.com/confluentinc/schema-registry/issues/877
        // Passing Schema Registry URL twice to instantiate KafkaAvroSerializer or Serde

        final Serde<String> stringSerde = Serdes.String();
        final GenericAvroSerde serde = new GenericAvroSerde(schemaRegistryClient);

        final Map<String, String> schema =
                Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        serde.configure(schema, false);

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, GenericRecord> stream = builder.stream(topicIn);
        stream.to(topicOut, Produced.with(stringSerde, serde));

        try (final TopologyTestDriver topologyTestDriver = new TopologyTestDriver(builder.build(), properties)) {
            //
            // Step 2: Setup input and output topics.
            //
            final TestInputTopic<String, Object> input = topologyTestDriver
                    .createInputTopic(topicIn,
                            new StringSerializer(),
                            new KafkaAvroSerializer(schemaRegistryClient));
            final TestOutputTopic<String, Object> output = topologyTestDriver
                    .createOutputTopic(topicOut,
                            new StringDeserializer(),
                            new KafkaAvroDeserializer(schemaRegistryClient));


            final List<Object> inputValues = Collections.singletonList(createAvroRecord("1"));

            //
            // Step 3: Produce some input data to the input topic.
            //
            input.pipeValueList(inputValues);

            //
            // Step 4: Verify the application's output data.
            //
            assertThat(output.readValuesToList(), equalTo(inputValues));
        } finally {
            MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
        }


//
//
//
//        // get topology
//        Topology topology =
////                RideShareStreamerV4.buildStream(topicIn, topicOut);
//        buildStream(topicIn, topicOut);
//        testDriver = new TopologyTestDriver(topology, properties);
//
//        TestInputTopic<String, GenericRecord> inputTopic = testDriver.createInputTopic(topicIn, new StringSerializer(), serde.serializer());
////        TestInputTopic<String, String> inputTopic = testDriver.createInputTopic(topicIn, new StringSerializer(), new StringSerializer());
//        inputTopic.pipeInput("1", createAvroRecord("1"));
////        inputTopic.pipeInput("2", createAvroRecord("2"));
////        inputTopic.pipeInput("2", "2");
//
////        TestOutputTopic<String, GenericRecord> outputTopic1 = testDriver.createOutputTopic(topicOut, new StringDeserializer(), serde.deserializer());
//        TestOutputTopic<String, String> outputTopic1 = testDriver.createOutputTopic(topicOut, new StringDeserializer(), new StringDeserializer());
////        KeyValue<String, String> record1 = outputTopic1.readKeyValue();
//
//        assertThat(outputTopic1.readKeyValue(), equalTo(new KeyValue<>("2", "CMT")));
//        assertThat(outputTopic1.isEmpty(), is(true));


//        /** Assert */
//        assertEquals("CMT", outRecord1.value().get("vendor_name"));
//        assertEquals("VTS", outRecord2.value().get("vendor_name"));
    }


    private GenericRecord createAvroRecord(String vname) throws IOException {
//        String userSchema = "{\"namespace\": \"com.insight\"," +
//                "\"type\": \"record\"," +
//                "\"name\": \"Ride4\"," +
//                "\"fields\": [" +
//                "{\"name\": \"vendor_name\", \"type\": \"string\", \"default\": \"\"}" +
//            "{\"name\": \"Trip_Pickup_DateTime\", \"type\": \"string\", \"default\": \"\"},"+
//            "{\"name\": \"Trip_Dropoff_DateTime\", \"type\": \"string\", \"default\": \"\"},"+
//            "{\"name\": \"Passenger_Count\", \"type\": \"int\", \"default\": 0},"+
//            "{\"name\": \"Trip_Distance\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Start_Lon\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Start_Lat\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Rate_Code\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"store_and_forward\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"End_Lon\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"End_Lat\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Payment_Type\", \"type\": \"string\", \"default\": \"\"},"+
//            "{\"name\": \"Fare_Amt\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Extra\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"mta_tax\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Tip_Amt\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Tolls_Amt\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"surcharge\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Total_Amt\", \"type\": \"double\", \"default\": 0.0},"+
//            "{\"name\": \"Process_time\", \"type\": \"string\", \"default\": \"\"}"+
//                "] }";

//        Schema.Parser parser = new Schema.Parser();
//        Schema schema = parser.parse(userSchema);


        final Schema schema = new Schema.Parser().parse(
                getClass().getResourceAsStream("/com/insight/Ride4.avsc")
        );
        GenericRecord avroRecord = new GenericData.Record(schema);
        avroRecord.put("vendor_name", vname);
        return avroRecord;
    }

    static Topology buildStream(String topicin, String topiout) {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> rideStream = builder.stream(topicin);

        KStream<String, String> processedStream = rideStream.mapValues(val -> val + "_NEW");

        processedStream.to(topiout);

        return builder.build();
    }

}
