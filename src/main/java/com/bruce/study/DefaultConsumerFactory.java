package com.bruce.study;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by bruce on 2018/3/29.
 */
public class DefaultConsumerFactory {

    private Consumer<String,String> kafkaConsumer;

    public Consumer<String,String> getInstance(String consumerGroup, boolean autoCommit) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9095");
        props.put("group.id", consumerGroup);
        props.put("enable.auto.commit", String.valueOf(autoCommit));
        if(autoCommit) {
            props.put("auto.commit.interval.ms", "1000");
        }
        props.put("auto.offset.reset","earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        kafkaConsumer = new KafkaConsumer<String, String>(props);
        return kafkaConsumer;
    }


}
