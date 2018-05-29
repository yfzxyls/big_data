import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class WordCountTopology {

    private static final String SPOUT_ID = "sentenceSpout";
    private static final String SPLIT_BOLT = "splitBolt";
    private static final String COUNT_BOLT = "countBolt";
    private static final String PRINT_BOLT = "printBolt";

    public static void main(String[] args) {
        // 构造Topology
        TopologyBuilder builder = new TopologyBuilder();
        // SPOUT_ID--> SPLIT_BOLT --> COUNT_BOLT --> PRINT_BOLT
        // 指定spout;
        //parallelism_hint:execute数量，多个task可以共用一个
        builder.setSpout(SPOUT_ID, new SentenceSpout());
        // 指定bolt，并指定当有有多个bolt时，数据流发射的分组策略;
        // setNumTasks(2) 可以设置多task并行执行
        builder.setBolt(SPLIT_BOLT, new SplitBolt()).shuffleGrouping(SPOUT_ID);//.setNumTasks(2);
        // 因为要保证正确的单词计数，同一个单词一定要划分到同一个CountBolt上，所以按照字段值分组
        builder.setBolt(COUNT_BOLT, new CountBolt(),2)
                //.shuffleGrouping(SPLIT_BOLT).setNumTasks(2);
                .fieldsGrouping(SPLIT_BOLT, new Fields("word")).setNumTasks(2);
        // 全局分组，所有tuple发射到一个printbolt，一般是id最小的那一个
        builder.setBolt(PRINT_BOLT, new PrintBolt()).globalGrouping(COUNT_BOLT);

        Config conf = new Config();
//        可以设置多worker默认1个
        conf.setNumWorkers(2);
        // 本地执行
//        LocalCluster localCluster = new LocalCluster();
//        localCluster.submitTopology("wordcount", conf, builder.createTopology());


        // 提交脚本： /Users/soapy/soft/apache-storm-1.2.1/bin/storm jar /Users/soapy/IdeaProjects/data-jobs/storm/study/target/study-1.0-SNAPSHOT.jar WordCountTopology
        try {
            StormSubmitter.submitTopology("wordcount",conf,builder.createTopology());
        } catch (AlreadyAliveException e) {
            e.printStackTrace();
        } catch (InvalidTopologyException e) {
            e.printStackTrace();
        } catch (AuthorizationException e) {
            e.printStackTrace();
        }
//        localCluster.killTopology("wordcount");
//        localCluster.shutdown();

    }
}
