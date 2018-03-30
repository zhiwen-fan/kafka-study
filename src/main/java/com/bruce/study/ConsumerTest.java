package com.bruce.study;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;

/**
 * Created by bruce on 2018/3/29.
 */
public class ConsumerTest {
    public static void main(String[] args) {
        try {
            //consumeWithAutoCommitMode();
            consumeWithManualCommitMode();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void consumeWithAutoCommitMode() {
        Consumer consumer = new DefaultConsumerFactory().getInstance("test-group-1",true);
        consumer.subscribe(Arrays.asList("bruce-topic-1"));

        while (true) {
            OffsetAndMetadata committed = consumer.committed(new TopicPartition("bruce-topic-1", 0));
            System.out.println("committed offset: " + committed.offset() + ", metadata: " + committed.metadata());
            System.out.println("start to consume message");
            ConsumerRecords<String, String> records = consumer.poll(0);
            System.out.println("get message from kafka");
            if(records.isEmpty()) {
                try {
                    System.out.println("wait 1000 ms");
                    Thread.sleep(1000);
                    continue;

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            System.out.println("start to handle message");
            for (ConsumerRecord<String,String> record: records) {
                System.out.printf("topic = %s, partition = %s, offset = %s, key = %s, value = %s",
                        record.topic(),record.partition(),record.offset(),record.key(),record.value());
                System.out.println();
            }
        }

    }

    private static void consumeWithManualCommitMode() {
        Consumer consumer = new DefaultConsumerFactory().getInstance("test-group-2",false);
        consumer.subscribe(Arrays.asList("bruce-topic-1"));

        while (true) {
            OffsetAndMetadata committed = consumer.committed(new TopicPartition("bruce-topic-1", 0));
            if(null != committed) {
                System.out.println("committed offset: " + committed.offset() + ", metadata: " + committed.metadata());
            }
            ConsumerRecords<String, String> records = consumer.poll(1);
            if(records.isEmpty()) {
                try {
                    System.out.println("wait 1000 ms");
                    Thread.sleep(1000);

                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            for (ConsumerRecord<String,String> record: records) {
                System.out.printf("topic = %s, partition = %s, offset = %s, key = %s, value = %s",
                        record.topic(),record.partition(),record.offset(),record.key(),record.value());
                System.out.println();
               // consumer.commitAsync(Collections.singletonMap(new TopicPartition(record.topic(),record.partition()),));
            }
            consumer.commitSync();
        }
    }
 }
