package com.alleyz.kafka.example.topic;

import com.alleyz.kafka.util.KafkaHelper;
import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.security.JaasUtils;

import java.util.Properties;

import static com.alleyz.kafka.Constant.ZOOKEEPER_CONNECT;

/**
 * Created by alleyz on 2017/4/27.
 *
 */
@Log4j
public class TopicHelper {

    private static TopicHelper helper;
    public static TopicHelper getInstance() {
        if(helper == null) {
            synchronized (TopicHelper.class) {
                if(helper == null) helper = new TopicHelper();
            }
        }
        return helper;
    }
    private ZkUtils zkUtils;
    private TopicHelper(){
        this.zkUtils = ZkUtils.apply(KafkaHelper.getProps(ZOOKEEPER_CONNECT), 30000, 30000,
                JaasUtils.isZkSecurityEnabled());
    }

    /**
     * 创建主题
     * @param topicName  主题名称
     * @param partitions 分区数
     * @param replicationFactor 副本数
     */
    public void createTopic(String topicName, int partitions, int replicationFactor, Properties prop) {
        if(prop == null) prop = new Properties();
        AdminUtils.createTopic(zkUtils, topicName, partitions, replicationFactor, prop, RackAwareMode.Enforced$.MODULE$);
    }

    public void createTopic(String topicName, int partitions, int replicationFactor) {
        createTopic(topicName, partitions, replicationFactor, null);
    }

    /**
     * 删除主题(marked for deletion)
     *  如果broker配置为：delete.topic.enable=false 则将此主题标记为 marked for deletion（如果要彻底移除，删除zk/broker内容）
     *  如果 delete.topic.enable=true 则将此主题彻底移除；
     * @param topicName 主题名称
     */
    public void delTopic(String topicName) {
        AdminUtils.deleteTopic(zkUtils, topicName);
    }

    /**
     * 获取主题元数据
     * @param topicName 主题名称
     * @return 元数据
     */
    public MetadataResponse.TopicMetadata queryTopic(String topicName) {
        return AdminUtils.fetchTopicMetadataFromZk(topicName, zkUtils);
    }

    /**
     * 判断主题是否存在
     * @param topicName 主题名称
     * @return true 存在
     */
    public boolean existTopic(String topicName) {
        MetadataResponse.TopicMetadata meta = queryTopic(topicName);
        return !Errors.UNKNOWN_TOPIC_OR_PARTITION.equals(meta.error());
    }

    /**
     * 更新主题配置（只需要更新需要改变的属性，其余属性会进行合并）
     * @param topic 主题
     * @param props 配置
     */
    public void updateTopic(String topic, Properties props) {
        Properties orgProps = AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic(), topic);
        props.entrySet().forEach(entry -> orgProps.setProperty((String)entry.getKey(), (String)entry.getValue()));
        AdminUtils.changeTopicConfig(zkUtils, topic, orgProps);
    }

    /**
     * 关闭zk连接
     */
    public void close() {
        if(this.zkUtils != null)
            zkUtils.close();
    }

}
