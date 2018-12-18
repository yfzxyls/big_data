package com.soap.storm.drpc;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

import java.util.Map;

/**
 * @author yangfuzhao on 2018/12/17.
 */
public class LocalDRPC {

    public static void main(String[] args) {
        LocalDRPC drpc = new LocalDRPC();
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        TopologyBuilder builder = new TopologyBuilder();
        cluster.submitTopology("",config,builder.createTopology());

    }
}
