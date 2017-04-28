package com.alleyz.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.TupleHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by zhaihw on 2017/4/25.
 *
 */
public class MyBasicBolt implements IBasicBolt {
    private static Logger LOGGER = LoggerFactory.getLogger(MyBasicBolt.class);
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        System.out.println("MyBasicBolt - prepare");
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        LOGGER.info("MyBasicBolt - execute");
        if (TupleHelpers.isTickTuple(input)) {
            LOGGER.info("MyBasicBolt - Receive one Ticket Tuple " + input.getSourceComponent());
            return;
        }
        if("MySpoutStreamId".equals(input.getSourceStreamId())){
            LOGGER.info("MyBasicBolt - receive spout msg is " + input.getLongByField("CtlMsg"));
            return;
        }
        long tupleId = (long) input.getValue(0);
        MyBean bean = (MyBean) input.getValue(1);
        LOGGER.info("MyBasicBolt -> tupleId = " + tupleId + " | bean = " + bean.getTxt());
//        collector.emit()
//        collector.
    }

    @Override
    public void cleanup() {
        LOGGER.info("MyBasicBolt - cleanup");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
//        declarer.
        LOGGER.info("MyBasicBolt - declareOutputFields");
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        LOGGER.info("MyBasicBolt - getComponentConfiguration");
        return null;
    }
}
