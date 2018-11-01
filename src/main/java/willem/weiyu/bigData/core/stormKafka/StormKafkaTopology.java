package willem.weiyu.bigData.core.stormKafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @Author weiyu005
 * @Description
 * @Date 2018/11/1 17:12
 */
public class StormKafkaTopology {
    public static final String TOPOLOGY_NAME = "storm-kafka-topology";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("kafka-spout",new KafkaSpout<>(KafkaSpoutConfig.builder("10.26.27.81:9092","test").build()));
        builder.setBolt("printBolt",new PrintBolt()).shuffleGrouping("kafka-spout");

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        //集群下使用StormSubmitter提交拓扑
        //StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    }
}
