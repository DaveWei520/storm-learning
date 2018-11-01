package willem.weiyu.bigData.trident;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.testing.FixedBatchSpout;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * @author weiyu
 * @description
 * @create 2017/6/28
 */
public class MergeTrident {

    public static void main(String[] args){
        Config config = new Config();
        config.setMaxSpoutPending(20);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wordCounter", config, buildTopology());
    }

    public static StormTopology buildTopology() {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("id", "sentence"), 3, new Values("1", "the cow jumped over the moon"),
                new Values("2", "the man went to the store and bought some candy"),
                new Values("1", "four score and seven years ago"),
                new Values("2", "how many apples can you eat"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();

        Stream totalStream = topology.newStream("spout1", spout);

        Stream stream1 = totalStream.each(new Fields("id", "sentence"), new BaseFilter() {
            public boolean isKeep(TridentTuple tuple) {
                return "1".equals(tuple.getString(0));
            }
        });

        Stream stream2 = totalStream.each(new Fields("id", "sentence"), new BaseFilter() {
            public boolean isKeep(TridentTuple tuple) {
                return "2".equals(tuple.getString(0));
            }
        });

        topology.merge(stream1,stream2).each(new Fields("id", "sentence"), new BaseFunction() {
            public void execute(TridentTuple tuple, TridentCollector collector) {
                System.out.println("tuple:"+tuple);
                System.out.println("======");
            }
        },new Fields("a","b","c"));
        /*topology.join(totalStream, new Fields("id"), totalStream, new Fields("id"), new Fields("id", "1s", "2s")).each(new Fields("id", "1s", "2s"), new BaseFilter() {
            public boolean isKeep(TridentTuple tuple) {
                System.out.println(tuple);
                System.out.println("======");
                return true;
            }
        });*/
        return topology.build();
    }
}
