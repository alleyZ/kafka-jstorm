package com.alleyz.kafka.example.consumer;


import com.alleyz.kafka.example.expection.NoBuilderException;
import com.alleyz.kafka.util.KafkaHelper;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import static com.alleyz.kafka.Constant.POLL_INTERVAL_MS;

/**
 * Created by alleyz on 2017/4/28.
 *
 */
@Log4j
public class ManualConsumer<K, V> extends AutoConsumer<K, V> {

    public ManualConsumer(String group, String ... topic){
        super(group, topic);
    }

    @Override
    public void startAccept(Handler<K, V> handler) throws NoBuilderException {
        if(this.consumer == null) {
            log.error("AutoConsumer - startAccept - 【失败】：请先执行buildConsumer方法建立consumer");
            throw new NoBuilderException("须先执行buildConsumer方法建立consumer");
        }
        while (super.isAccept.get()) {
            log.debug("AutoConsumer - startAccept: 接受消息");
            ConsumerRecords<K, V> crs = this.consumer.poll(KafkaHelper.getLongVal(POLL_INTERVAL_MS, this.props, 1000L));
            if(!crs.isEmpty()) {
                handler.process(crs);
                this.consumer.commitSync(); // 同步提交offset
            }

        }
    }
}
