package com.alleyz.kafka;

/**
 * Created by alleyz on 2017/4/27.
 *
 */
public final class Constant {
    private Constant(){}
    // 主题名称
    public final static String TOPIC = "alleyz";

    public final static String DEFAULT_GROUP = "defaultGroup";

    public final static String CONSUMER_GROUP_ID = "group.id";

    public final static String ZOOKEEPER_CONNECT = "zookeeper.connect";

    public final static String POLL_INTERVAL_MS = "poll.interval.ms";
}
