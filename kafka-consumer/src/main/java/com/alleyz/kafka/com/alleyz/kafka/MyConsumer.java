package com.alleyz.kafka.com.alleyz.kafka;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by zhaihw on 2017/4/20.
 */
public class MyConsumer extends Thread{
    private final ConsumerConnector consumer;
    private final String topic;
    public MyConsumer(String topic)
    {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig());
        this.topic = topic;
    }
    private static ConsumerConfig createConsumerConfig()
    {
        Properties props = new Properties();
        props.put("zookeeper.connect", "10.8.177.23:2181,10.8.177.24:2181,10.8.177.25:2181");
        props.put("group.id", "g3");
        props.put("zookeeper.session.timeout.ms", "40000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
//        consumer.se
//        Map<TopicAndPartition, OffsetAndMetadata> offsetMap = new HashMap<>();
//        TopicAndPartition partition = new TopicAndPartition(topic, 0);
//        OffsetMetadata metadata2 = new OffsetMetadata(1522, "msg");
//        OffsetAndMetadata offset = new OffsetAndMetadata(metadata2, 11111111L, 22222L);
//        offsetMap.put(partition, offset);
//        consumer.commitOffsets(offsetMap, false);

        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        System.out.println("consumerMap size:" + consumerMap.get(topic).size());
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        while (it.hasNext()) {
            MessageAndMetadata<byte[], byte[]> metadata = it.next();
//            long offset = metadata.offset();
            System.out.println("offset=" + metadata.offset() + " partition=" + metadata.partition() +
                    " key=" + new String(metadata.key()) +
                    " message=" + new String(metadata.message()));
//            consumer.commitOffsets();

        }
    }
}
