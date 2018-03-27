package com.bruce.study;

/**
 * Created by bruce on 2018/3/27.
 */
public class ProducerTest {
    public static void main(String[] args) {
        DefaultProducer producer = new DefaultProducer();
        String topic = "bruce-topic-1";

        for (int i=0; i<1; i++) {
            producer.sendMessage(topic, String.valueOf(i));
        }
    }
}
