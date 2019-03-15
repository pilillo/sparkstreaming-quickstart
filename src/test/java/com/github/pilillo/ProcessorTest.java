package com.github.pilillo;

import com.github.pilillo.kafka.EmbeddedSingleNodeKafkaCluster;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;

public class ProcessorTest extends SharedJavaSparkContext
        implements Serializable
{

    private static final long serialVersionUID = 946237749385607222L;

    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final String inputTopic = "inputTopic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(inputTopic);
    }

    @Test
    public void pipelineTest(){
        // **** Kafka topic producer ****

        // https://docs.confluent.io/current/streams/developer-guide/datatypes.html#avro
        final Map<String, String> serdeCfg = Collections.singletonMap("schema.registry.url", CLUSTER.schemaRegistryUrl());
        final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
        keyGenericAvroSerde.configure(serdeCfg, true); // `true` for record keys
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeCfg, false); // `false` for record values

        // Producer side
        Properties consProps = new Properties();
        consProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        KafkaProducer<String,GenericRecord> prod = new KafkaProducer<>(consProps,
                Serdes.String().serializer(),
                valueGenericAvroSerde.serializer());

        // create schema for data to ingest in kafka (it will be also pushed to the schema registry)
        Schema schema = SchemaBuilder
                .record("testschema").namespace("org.apache.avro.ipc")
                .fields()
                .name("name").type().stringType().noDefault()
                .name("age").type().nullable().intType().noDefault()
                .endRecord();
        GenericRecord r_v = new GenericData.Record(schema);
        r_v.put("name", "Gilberto");
        r_v.put("age", 59);


        ProducerRecord<String,GenericRecord> producerRecord = new ProducerRecord<String, GenericRecord>(inputTopic, "key", r_v);
        Future<RecordMetadata> fp = prod.send(producerRecord);
        prod.flush();
        prod.close();


        // **** Spark processing ****

        // set a time limit before interrupting the tests
        long testDuration = 30000L;

        String checkpointFolder = "tests_results";
        long batchPeriod = 1000L;

        Properties props = new Properties();
        props.setProperty("name", "test-app");
        props.setProperty("zookeeper", CLUSTER.bootstrapServers());
        props.setProperty("registry", CLUSTER.schemaRegistryUrl());
        props.setProperty("earliest", "true");
        props.setProperty("source_topic", inputTopic);

        JavaStreamingContext jssc = new JavaStreamingContext(this.jsc(), Durations.milliseconds(batchPeriod));
        jssc.checkpoint(checkpointFolder);

        Processor proc = Processor.getInstance(jssc, props);
        JavaInputDStream<ConsumerRecord<String, GenericRecord>>
                iStream = proc.openStream(Arrays.asList(inputTopic),
                                           Serdes.String().deserializer().getClass().getName(),
                                            KafkaAvroDeserializer.class.getName()
        );
        proc.processStream(iStream);

        // Start the computation
        jssc.start();

        // Wait for the computation to terminate
        try {
            jssc.awaitTerminationOrTimeout(testDuration);
        } catch (InterruptedException e) {
            System.out.println("Timeout rang, end of processing!");
        }

        jssc.close();

        // delete checkpoint folder
        for(File f : (new File(checkpointFolder)).listFiles()){
            System.out.println("Removing "+f.getPath());
            f.delete();
        }
        (new File(checkpointFolder)).delete();
    }


}
