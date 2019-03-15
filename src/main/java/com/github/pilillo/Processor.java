package com.github.pilillo;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.Arguments;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function0;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public class Processor {

    private static Processor instance;

    private static String appName;
    private static String zookeeper;
    private static String schemaRegistry;
    private static boolean fromBeginning;
    private static String checkpointFolder;
    private static long batchPeriod;
    private static String sourceTopic;

    private static SparkConf conf;
    private static JavaSparkContext sc;
    private static JavaStreamingContext jssc;

    private Function0<JavaStreamingContext> getFactory(){

        return (Function0<JavaStreamingContext>) () -> {
            JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(batchPeriod));

            // set checkpoint directory
            jssc.checkpoint(checkpointFolder);

            // open input stream anew
            JavaInputDStream<ConsumerRecord<String, GenericRecord>> messages = openStream(
                    Arrays.asList(sourceTopic),
                    Serdes.String().deserializer().getClass().getName(),
                    KafkaAvroDeserializer.class.getName()
            );

            return jssc;
        };
    }


    private Processor(){
        if(jssc == null){
            conf = new SparkConf().setAppName(appName);
            // without stateful operations (no checkpoint needed), or for testing
            //jssc = new JavaStreamingContext(conf, Durations.milliseconds(batchPeriod));
            // with stateful operations (checkpointing and singleton creation)
            jssc = JavaStreamingContext.getOrCreate(checkpointFolder, getFactory());
        }
        sc = jssc.sparkContext();
    }

    public static Processor getInstance(JavaStreamingContext existingjssc, Properties p){
        if(instance == null){
            jssc = existingjssc;

            appName = p.getProperty("name");
            zookeeper = p.getProperty("zookeeper");
            schemaRegistry = p.getProperty("registry");
            fromBeginning = Boolean.parseBoolean(p.getProperty("earliest").equals("")?"false":p.getProperty("earliest"));
            checkpointFolder = p.getProperty("checkpoint_folder");
            sourceTopic = p.getProperty("source_topic");

            instance = new Processor();
        }
        return instance;
    }


    public static Processor getInstance(String[] args){
        if(instance == null){
            parseInput(args);
            instance = new Processor();
        }
        return instance;
    }

    private static void parseInput(String[] args){
        ArgumentParser parser = ArgumentParsers.newFor("StreamProcessor")
                                            .build()
                                            .description("An example processor in Spark Streaming");

        parser.addArgument("--name").type(String.class).help("application name");
        parser.addArgument("--zookeeper").type(String.class).help("zookeeper host:port (e.g. localhost:9092,anotherhost:9092)");
        parser.addArgument("--registry").type(String.class).help("confluent schema registry host:port");
        parser.addArgument("--earliest").action(Arguments.storeTrue()).help("whether to start from retention beginning");
        parser.addArgument("--checkpoint-folder").type(String.class).help("path for Spark streaming checkpoints");
        parser.addArgument("--batch-period").type(Long.class).help("ms period size for microbatches");
        parser.addArgument("--source-topic").type(String.class).help("source topic");

        Namespace ns = parser.parseArgsOrFail(args);
        appName = ns.getString("name");
        zookeeper = ns.getString("zookeeper");
        schemaRegistry = ns.getString("registry");
        fromBeginning = ns.getBoolean("earliest");
        checkpointFolder = ns.getString("checkpoint_folder");
        batchPeriod = ns.getLong("batch_period");
        sourceTopic = ns.getString("source_topic");
    }

    public JavaInputDStream<ConsumerRecord<String, GenericRecord>> openStream(
            List<String> topics,
            String keyDeserializer,
            String valueDeserializer
    ){
        // https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", zookeeper);
        kafkaParams.put("key.deserializer", keyDeserializer);
        kafkaParams.put("value.deserializer", valueDeserializer);
        // use the schema registry url if set, as we need this for avro serialization
        if(schemaRegistry != null)
            kafkaParams.put("schema.registry.url", schemaRegistry);
        kafkaParams.put("group.id", appName+"-consumers");
        kafkaParams.put("auto.offset.reset", fromBeginning?"earliest":"latest");
        kafkaParams.put("enable.auto.commit", false);
        // we want to commit the offsets explicitly by ourselves
        // https://spark.apache.org/docs/latest/streaming-kafka-0-10-integration.html#kafka-itself
        return KafkaUtils.createDirectStream(jssc,
                                            LocationStrategies.PreferConsistent(),
                                            ConsumerStrategies.Subscribe(topics, kafkaParams));
    }

    public void processStream(JavaInputDStream<ConsumerRecord<String, GenericRecord>> iStream){
        iStream.foreachRDD(rdd -> {
                    rdd.foreach(avroRecord -> {
                        System.out.println("k:"+avroRecord.key()+", v:"+avroRecord.value());
                });
        });
    }

    private void startPipeline() throws InterruptedException {
        // open stream to kafka
        JavaInputDStream<ConsumerRecord<String, GenericRecord>> iStream = openStream(
                Arrays.asList(sourceTopic),
                Serdes.String().deserializer().getClass().getName(),
                KafkaAvroDeserializer.class.getName()
        );
        // add stream processor
        processStream(iStream);

        // Start the computation
        jssc.start();
        // Wait for the computation to terminate
        jssc.awaitTermination();
    }

    public static void main(String[] args) throws InterruptedException {
        // create a pipeline processor from the passed arguments
        Processor p = Processor.getInstance(args);

        // start pipeline
        p.startPipeline();
    }
}
