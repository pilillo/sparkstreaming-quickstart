package com.github.pilillo;

import com.github.pilillo.kafka.EmbeddedSingleNodeKafkaCluster;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.*;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.Future;

public class KafkaTestExample {
    @ClassRule
    public static final EmbeddedSingleNodeKafkaCluster CLUSTER = new EmbeddedSingleNodeKafkaCluster();

    private static final String stringTopic = "stringTopic";
    private static final String avroTopic = "avroTopic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(stringTopic);
        CLUSTER.createTopic(avroTopic);
    }

    private Properties getCfg(boolean consumer){
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        if(consumer){
            p.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
            p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        }
        return p;
    }

    private KafkaProducer<String, String> getStringProducer(){
        return new KafkaProducer<>(getCfg(false), Serdes.String().serializer(), Serdes.String().serializer());
    }

    private KafkaConsumer<String, String> getStringConsumer(){
        return new KafkaConsumer<>(getCfg(true), Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @Test
    public void stringTopicTest(){
        // Producer side
        KafkaProducer<String,String> p = getStringProducer();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(stringTopic,"key", "value");
        //new ProducerRecord<>(stringTopic, partition, time, key, value);
        // send example record
        Future<RecordMetadata> f = p.send(producerRecord);
        p.flush();
        p.close();

        // Consumer side
        KafkaConsumer<String, String> c = getStringConsumer();
        c.subscribe(Collections.singletonList(stringTopic));
        List<ConsumerRecord<String,String>> consumed = new ArrayList<>();
        long timeout = System.currentTimeMillis() + 10000L;
        boolean done = false;
        while(!done && System.currentTimeMillis() < timeout) {
            ConsumerRecords<String,String> polled = c.poll(10);
            polled.forEach(record -> {
                consumed.add(record);
                System.out.println("key: "+record.key()+ ", value: "+record.value());
            });
            done = consumed.size()==1;
        }
        c.close();
    }


    @Test
    public void avroTopicTest(){
        // https://docs.confluent.io/current/streams/developer-guide/datatypes.html#avro
        final Map<String, String> serdeCfg = Collections.singletonMap("schema.registry.url", CLUSTER.schemaRegistryUrl());

        final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
        // `true` for record keys
        keyGenericAvroSerde.configure(serdeCfg, true);

        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        // `false` for record values
        valueGenericAvroSerde.configure(serdeCfg, false);

        // Producer side
        KafkaProducer<String,GenericRecord> p = new KafkaProducer<>(getCfg(false),
                                                                Serdes.String().serializer(),
                                                                //keyGenericAvroSerde.serializer(),
                                                                valueGenericAvroSerde.serializer());
        //Schema schema =  Schema.createRecord("nestedrecord", null, null, false);
        //schema.setFields(Lists.newArrayList(buildField("myint", Schema.Type.INT)));
        Schema schema = SchemaBuilder
                .record("testschema").namespace("org.apache.avro.ipc")
                        .fields()
                        .name("name").type().stringType().noDefault()
                        .name("age").type().nullable().intType().noDefault()
                        .endRecord();


        GenericRecord r_v = new GenericData.Record(schema);
        r_v.put("name", "Gilberto");
        r_v.put("age", 59);

        ProducerRecord<String,GenericRecord> producerRecord = new ProducerRecord<String, GenericRecord>(avroTopic, "key", r_v);

        Future<RecordMetadata> f = p.send(producerRecord);
        p.flush();
        p.close();

        // Consumer side
        KafkaConsumer<String,GenericRecord> c = new KafkaConsumer<>(getCfg(true),
                                                                Serdes.String().deserializer(),
                                                                //keyGenericAvroSerde.deserializer(),
                                                                valueGenericAvroSerde.deserializer());
        c.subscribe(Collections.singletonList(avroTopic));

        List<ConsumerRecord<String,GenericRecord>> consumed = new ArrayList<>();
        long timeout = System.currentTimeMillis() + 10000L;
        boolean done = false;
        while(!done && System.currentTimeMillis() < timeout) {
            ConsumerRecords<String,GenericRecord> polled = c.poll(10);
            polled.forEach(record -> {
                consumed.add(record);
                System.out.println("key: "+record.key()+ ", value: "+record.value());
            });
            done = consumed.size()==1;
        }
        c.close();


    }

}
