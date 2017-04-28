package com.alleyz.kafka;

import com.alleyz.kafka.example.consumer.AutoConsumer;
import com.alleyz.kafka.example.consumer.ManualConsumer;
import com.alleyz.kafka.example.expection.NoBuilderException;
import com.alleyz.kafka.example.expection.NoTopicException;
import com.alleyz.kafka.example.producer.NormalProducer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.util.Iterator;

import static com.alleyz.kafka.Constant.TOPIC;

/**
 * Created by alleyz on 2017/4/27.
 *
 */
public class KafkaMain {

    private static void autoCommitConsumer (String group) {
        try {
            // todo 这里居然不加参数也可以new ！！
            AutoConsumer<String, String> autoConsumer = new ManualConsumer<>(group, TOPIC);
            autoConsumer.buildConsumer("/auto-consumer.properties");
            autoConsumer.startAccept(crs -> {
                System.out.println("KafkaMain - autoCommitConsumer: 处理消息" + crs.partitions());
                if(crs.isEmpty()) {
                    try {
                        Thread.sleep(1000);
                    }catch (Exception e) {e.printStackTrace();}
                    return;
                }
                Iterator<ConsumerRecord<String, String>> recordIterable = crs.records(TOPIC).iterator();
                System.out.println("KafkaMain - autoCommitConsumer" + crs.count());
                recordIterable.forEachRemaining(record -> {
                    System.out.println(record.key() + "----" + record.value() + " ---- ");
                });
            });
            System.out.println(" END ---! ");
        }catch (IOException |NoTopicException | NoBuilderException e) {
            e.printStackTrace();
        }
    }

    private static void sendMsg(Integer startNo) {
        if(startNo == null) startNo = 0;
        try {
            NormalProducer<String, String> producer = new NormalProducer<>(TOPIC);
            producer.builderProducer("/normal-producer.properties");
            for (int i = startNo; true; i++) {
                producer.sendMsg("k-" + i, "v=" + i, i % 4);
                Thread.sleep(200L);
            }
        }catch (IOException | NoTopicException | NoBuilderException | InterruptedException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) {
//        TopicHelper.getInstance().createTopic(Constant.TOPIC, 4, 2);
//        TopicHelper.getInstance().delTopic(Constant.TOPIC);
//        System.out.println(TopicHelper.getInstance().existTopic(Constant.TOPIC));
//        sendMsg(4867);
//        autoCommitConsumer("group-01");
        autoCommitConsumer("group-03");
    }
}
