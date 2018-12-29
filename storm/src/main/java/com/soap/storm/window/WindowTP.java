package com.soap.storm.window;

import com.soap.storm.core.SentenceSpout;
import com.soap.storm.core.SplitBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

/**
 * @author yangfuzhao on 2018/12/26.
 */
public class WindowTP {


    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new SentenceSpout(), 1);
        // 设置窗口的Sliding interval 和 Window length

        builder.setBolt("split",new SplitBolt()).shuffleGrouping("spout");

        builder.setBolt("slidingwindowbolt", new SlidingWindowBolt().withWindow(new BaseWindowedBolt.Count(2), new BaseWindowedBolt.Count(1)), 1)
                .shuffleGrouping("split");
        builder.setBolt("printBolt",new PrintBolt(),1).shuffleGrouping("slidingwindowbolt");
        Config conf = new Config();
//        conf.setDebug(true);
        conf.setNumWorkers(1);

        LocalCluster localCluster = new LocalCluster();

        localCluster.submitTopology("wordWindowCount", conf, builder.createTopology());

//        StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
}
