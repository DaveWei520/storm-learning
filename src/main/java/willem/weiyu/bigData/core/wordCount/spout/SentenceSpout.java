package willem.weiyu.bigData.core.wordCount.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * @author weiyu
 * @description
 * @create 2017/6/28
 */
public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] sentences = {
            "Apache Storm is a free and open source distributed realtime computation system",
            "Spark Streaming is an extension of the core Spark API that enables scalable high-throughput fault-tolerant stream processing of live data streams",
            "Apache Flink is a framework and distributed processing engine for stateful computations over unbounded and bounded data streams",
            "Apache Beam is an advanced unified programming model"};
    private Random rand;
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        rand = new Random();
    }

    @Override
    public void nextTuple() {
        collector.emit(new Values(sentences[rand.nextInt(sentences.length)]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentence"));
    }
}
