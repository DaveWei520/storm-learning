package willem.weiyu.bigData.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * @author weiyu
 * @description
 * @create 2017/6/28
 */
public class JoinTrident {

    public static void main(String[] args){
        Config config = new Config();
        config.setMaxSpoutPending(20);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", config, buildTopology());
    }

    public static StormTopology buildTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("id", "sentence","no"), 4, new Values("1", "the cow jumped over the moon","flag"),
                new Values("2", "the man went to the store and bought some candy","flag"),
                new Values("1", "four score and seven years ago","flag"),
                new Values("2", "how many apples can you eat","flag"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        Stream totalStream = topology.newStream("spout1", spout);

        Stream stream1 = totalStream.each(new Fields("id", "sentence","no"), new BaseFilter() {
            public boolean isKeep(TridentTuple tuple) {
                return "1".equals(tuple.getString(0));
            }
        });

        Stream stream2 = totalStream.each(new Fields("id", "sentence","no"), new BaseFilter() {
            public boolean isKeep(TridentTuple tuple) {
                return "2".equals(tuple.getString(0));
            }
        });

        topology.join(stream1, new Fields("no"), stream2, new Fields("no"), new Fields("no", "1id", "1sentence", "2id", "2sentence"))
                .each(new Fields("no", "1id", "1sentence", "2id", "2sentence"), new BaseFilter() {
            public boolean isKeep(TridentTuple tuple) {
                System.out.println(tuple);
                System.out.println("======");
                return true;
            }
        });
        return topology.build();
    }
}
