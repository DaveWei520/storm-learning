package willem.weiyu.bigData.core.stormKafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @Author weiyu005
 * @Description kafkaSpout对于ack的消息会调用kafkaConsumer.commitSync方法进行提交，将offset维护到kafka的内部队列__consumer_offsets中
 * @Date 2018/11/1 17:12
 */
public class StormKafkaTopology {
    public static final String TOPOLOGY_NAME = "storm-kafka-topology";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpoutConfig conf = KafkaSpoutConfig.builder("10.26.27.81:9092","test")
                .setProp("group.id","weiyu")
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
                .build();

        builder.setSpout("kafka-spout",new KafkaSpout<>(conf));
        builder.setBolt("printBolt",new PrintBolt()).shuffleGrouping("kafka-spout");

        Config config = new Config();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        //集群下使用StormSubmitter提交拓扑
        //StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    }
}
