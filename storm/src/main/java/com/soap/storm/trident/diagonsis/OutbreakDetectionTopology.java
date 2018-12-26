package com.soap.storm.trident.diagonsis;


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.tuple.Fields;


/**
 * @author yangfuzhao on 2018/12/25.
 */
public class OutbreakDetectionTopology {
    public static StormTopology buildTopology() {
        TridentTopology topology = new TridentTopology();
        DiagonisiEventSpout spout = new DiagonisiEventSpout();
        Stream inputStream = topology.newStream("event", spout);


        inputStream.each(new Fields("event"), new DiseaseFilter())
                .each(new Fields("event"), new CityAssignment(), new Fields("city"))
                // functionFields 顺序必须与 HourAssignment 添加顺序一致一致
                .each(new Fields("event", "city"), new HourAssignment(), new Fields("hour", "cityDiseaseHour"))
                .groupBy(new Fields("cityDiseaseHour"))

                .persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count"))
//                .persistentAggregate(new OutbreakTrendFactory(), new Count(), new Fields("count"))
                .newValuesStream()
                .each(new Fields("cityDiseaseHour", "count"), new OutbreakDetector(), new Fields("alert"))
                .each(new Fields("alert"), new DispatchAlert(), new Fields());
        return topology.build();
    }

    public static void main(String[] args) throws Exception {
        Config conf = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("cdc", conf, buildTopology());
        Thread.sleep(200000);
        cluster.shutdown();
    }

}
