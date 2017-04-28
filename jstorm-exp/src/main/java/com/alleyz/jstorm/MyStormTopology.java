package com.alleyz.jstorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by zhaihw on 2017/4/24.
 *
 */
public class MyStormTopology {
    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_BOLT_PARALLELISM_HINT  = "bolt.parallel";

    private static Map<String, Object> conf = new HashMap<>();
    static {

    }

    public static void main(String[] args){
        try {
            TopologyBuilder topology = new TopologyBuilder();
            SpoutDeclarer spoutDeclarer = topology.setSpout("my-spout", new MySpout(), 1);
            BoltDeclarer boltDeclarer = topology.setBolt("my-bolt", new MyBasicBolt(), 3);

            boltDeclarer
                    .localOrShuffleGrouping("my-spout")
                    .allGrouping("my-spout", "MySpoutStreamId")
                    .addConfiguration("topology.tick.tuple.freq.secs", 3);
            conf.put("topology.acker.executors", 1);
            conf.put("topology.workers", 20);
            conf.put(Config.STORM_CLUSTER_MODE, "distributed");

//            LocalCluster cluster = new LocalCluster();
//            cluster.submitTopology("my-topology", conf, topology.createTopology());
            StormSubmitter.submitTopology("my-topology", conf, topology.createTopology());
//            Thread.sleep(60000);
//            cluster.killTopology("SplitMerge");
//            cluster.shutdown();


        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
