package com.alleyz.kafka.test;

import com.alleyz.kafka.com.alleyz.kafka.MyProducer;

/**
 * Created by zhaihw on 2017/4/20.
 */
public class Producer {
    public static void main(String[] args) {
        MyProducer producerThread = new MyProducer("test");
        producerThread.start();
//        MyConsumer consumerThread = new MyConsumer(KafkaProperties.topic);
//        consumerThread.start();
    }
}
