package com.alleyz.jstorm;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by zhaihw on 2017/4/25.
 *
 */
public class MySpout implements IRichSpout {
    private static final long serialVersionUID = 1L;
    public static final Logger LOGGER = LoggerFactory.getLogger(MySpout.class);

    private boolean isStopSend = false; // 停止发射数据
    private AtomicLong currentSend = new AtomicLong(0);
    private SpoutOutputCollector collector;
    private Random idRandom ;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        idRandom = new Random(Utils.secureRandomLong());
        LOGGER.info("MySpout - open");
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("MySpoutStreamId", new Fields("CtlMsg"));
        declarer.declare(new Fields("id", "record"));
        LOGGER.info("MySpout - declareOutputFields");
    }

    @Override
    public void nextTuple() {
        LOGGER.info("MySpout - nextTuple");
        if(isStopSend) {
            this.collector.emit("MySpoutStreamId", new Values(System.currentTimeMillis()), " MySpout send end!");
            return;
        }
        long tupleId = currentSend.incrementAndGet();
        this.collector.emit(new Values(currentSend.longValue(), new MyBean(idRandom.nextLong() + "", "I Love Love Love Nothing")), tupleId);
        if(currentSend.longValue() >= 1000L) {
            isStopSend = true;
        }

    }



    @Override
    public void activate() {
        LOGGER.info("MySpout - activate");
    }

    @Override
    public void deactivate() {
        LOGGER.info("MySpout - deactivate");
    }

    @Override
    public void ack(Object msgId) {
        LOGGER.info("MySpout - ack");
    }

    @Override
    public void fail(Object msgId) {
        LOGGER.info("MySpout - fail");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOGGER.info("MySpout - getComponentConfiguration");
        return null;
    }
    @Override
    public void close() {
        LOGGER.info("MySpout - close");
    }

}
