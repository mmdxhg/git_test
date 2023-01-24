package com.atguigu.gmall.realtime.util;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class KafkaUtil {

    public static String BOOTSTRAP_SERVERS = "hadoop102:9092,hadoop103:9092,hadoop104:9092";

    public static String DEFAULT_TOPIC = "default_topic";


    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers",BOOTSTRAP_SERVERS);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);

        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<String>(
                topic, new KafkaDeserializationSchema<String>() {
            @Override
            public boolean isEndOfStream(String nextElement) {
                return false;
            }

            @Override
            public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {

                if (record != null && record.value() != null){
                    return new String(record.value(),StandardCharsets.UTF_8);
                }
                return null;
            }

            @Override
            public TypeInformation<String> getProducedType() {
                return TypeInformation.of(String.class);
            }
        },properties
        );

        return stringFlinkKafkaConsumer;
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        Properties properties = new Properties();
        properties.setProperty(
                "bootstrap.servers",BOOTSTRAP_SERVERS
        );
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                60 * 15 * 1000 + "");

        FlinkKafkaProducer<String> stringFlinkKafkaProducer = new FlinkKafkaProducer<String>(
                DEFAULT_TOPIC, new KafkaSerializationSchema<String>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(String element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[], byte[]>(topic,element.getBytes());
            }
        },properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );

        return stringFlinkKafkaProducer;
    }
}