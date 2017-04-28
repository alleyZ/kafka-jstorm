package com.alleyz.kafka.example.consumer;

import com.alleyz.kafka.example.expection.NoBuilderException;
import com.alleyz.kafka.example.expection.NoTopicException;
import com.alleyz.kafka.util.KafkaHelper;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.alleyz.kafka.Constant.CONSUMER_GROUP_ID;
import static com.alleyz.kafka.Constant.DEFAULT_GROUP;
import static com.alleyz.kafka.Constant.POLL_INTERVAL_MS;

/**
 * Created by alleyz on 2017/4/27.
 * 自动提交offset
 */
@Log4j // 日志打印组件 lombok
public class AutoConsumer<K, V> {
    protected String[] topic;
    protected String group;
    protected KafkaConsumer<K, V> consumer;
    protected volatile AtomicBoolean isAccept = new AtomicBoolean(true); // 是否接受消息
    protected Properties props;
    public AutoConsumer(String group, String ... topic){
        this.topic = topic;
        this.group = group;
    }
    /**
     * 建立消费者
     * @param propsFile 配置文件
     * @throws IOException 异常
     */
    public void buildConsumer(String propsFile) throws IOException,NoTopicException {
        if(this.topic == null || this.topic.length == 0) {
            log.error("AutoConsumer - buildConsumer - 【失败】：未包含可订阅的主题!");
            throw new NoTopicException("未包含可订阅的主题!");
        }
        this.props = KafkaHelper.loadProps(propsFile);
        //如果代码中设置组，则代码中组优先
        if(!(this.group == null || "".equals(this.group))){
            this.props.put(CONSUMER_GROUP_ID, this.group);
        }else{ // 配置文件是否设置了组
            String consumerGroup = props.getProperty(CONSUMER_GROUP_ID);
            // 如果未配置组，则设置默认组
            if(consumerGroup == null || "".equals(consumerGroup)) {
                this.props.put(CONSUMER_GROUP_ID, DEFAULT_GROUP);
            }
        }
        this.consumer = new KafkaConsumer<>(this.props);
        this.consumer.subscribe(Arrays.asList(this.topic));
    }

    /**
     * 开始接受消息
     * @param handler 消息处理类
     */
    public void startAccept(Handler<K, V> handler) throws NoBuilderException{
        if(this.consumer == null) {
            log.error("AutoConsumer - startAccept - 【失败】：请先执行buildConsumer方法建立consumer");
            throw new NoBuilderException("须先执行buildConsumer方法建立consumer");
        }
        log.info("AutoConsumer - startAccept: 接受消息【开启】");
        while (isAccept.get()) {
            log.debug("AutoConsumer - startAccept: 接受消息");
            ConsumerRecords<K, V> crs = this.consumer.poll(KafkaHelper.getLongVal(POLL_INTERVAL_MS, this.props, 1000L));
            if(crs.isEmpty()) break;
            handler.process(crs);
        }
    }

    /**
     * 暂停接受消息，执行此方法后，最多能接收到一次消息；
     */
    public void pauseAccept() {
        this.isAccept.set(false);
        log.info("AutoConsumer - stopAccept: 接受消息【关闭】");
    }

    /**
     * 断开消费者
     */
    public void close() {
        if(this.consumer != null)
            this.consumer.close();
    }

    /**
     * 消息处理类
     * @param <K> k
     * @param <V> v
     */
    public interface Handler<K, V>{
        void process(ConsumerRecords<K, V> crs);
    }
}
