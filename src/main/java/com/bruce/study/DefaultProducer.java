package com.bruce.study;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Created by bruce on 2018/3/27.
 */
public class DefaultProducer {
    private Producer<String,String> kafkaProducer;

    DefaultProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9095");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        kafkaProducer = new KafkaProducer<String, String>(props);
    }


    public void sendMessage(String topic,String msg) {
        ProducerRecord<String,String> record = new ProducerRecord<String, String>(topic,msg,msg);
        Future<RecordMetadata> futureResult = kafkaProducer.send(record);
        try {
            RecordMetadata recordMetadata = futureResult.get();
            System.out.println("offset:" + recordMetadata.offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }
}
