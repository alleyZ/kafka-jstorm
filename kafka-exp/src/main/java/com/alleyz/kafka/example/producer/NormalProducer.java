package com.alleyz.kafka.example.producer;

import com.alleyz.kafka.example.expection.NoBuilderException;
import com.alleyz.kafka.example.expection.NoTopicException;
import com.alleyz.kafka.util.KafkaHelper;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * Created by alleyz on 2017/4/27.
 *
 */
@Log4j
public class NormalProducer<K, V> {
    private KafkaProducer<K, V> producer;
    private String topic;

    public NormalProducer(String topic){
        this.topic = topic;
    }

    public void builderProducer(String propsFile) throws IOException,NoTopicException {
        if(this.topic == null || "".equals(this.topic)) {
            log.error("NormalProducer - builderProducer - 【失败】：未包含可推送消息的主题!");
            throw new NoTopicException("未包含可推送消息的主题!");
        }
        Properties props = KafkaHelper.loadProps(propsFile);
        this.producer = new KafkaProducer<>(props);

    }

    /**
     * 发送消息(默认分区)
     * @param k key
     * @param v value
     */
    public void sendMsg(K k, V v) throws NoBuilderException{
        sendMsg(k, v, null);
    }

    /**
     * 发送消息 指定分区
     * @param k key
     * @param v value
     * @param partition 分区
     * @throws NoBuilderException
     */
    public void sendMsg(K k, V v, Integer partition) throws NoBuilderException{
        sendMsg(k, v, partition, null);
    }

    /**
     * 发送消息
     * @param k key
     * @param v value
     * @param partition 分区
     * @param timestamp 时间戳
     * @throws NoBuilderException
     */
    public void sendMsg(K k, V v, Integer partition, Long timestamp) throws NoBuilderException{
        if(this.producer == null) {
            log.error("NormalProducer - sendMsg - 【失败】：须先执行buildConsumer方法建立consumer");
            throw new NoBuilderException("须先执行buildConsumer方法建立consumer");
        }
        producer.send(new ProducerRecord<>(this.topic, partition, timestamp, k, v));
    }

    /**
     * 关闭生产者
     */
    public void close() {
        if(this.producer != null)
            this.producer.close();
    }
}
