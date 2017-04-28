package com.alleyz.kafka.test;

import com.alleyz.constant.KafkaProperties;
import com.alleyz.kafka.com.alleyz.kafka.MyConsumer;

/**
 * Created by zhaihw on 2017/4/21.
 */
public class Consumer2 {
    public static void main(String[] args) {
        MyConsumer myConsumer = new MyConsumer(KafkaProperties.topic);
        myConsumer.start();
    }
}
