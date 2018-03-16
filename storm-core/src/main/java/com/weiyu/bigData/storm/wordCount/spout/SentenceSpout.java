package com.weiyu.bigData.storm.wordCount.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class SentenceSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] sentences = {
            "Apache Storm is a free and open source distributed realtime computation system",
            "Apache Spark is a fast and general engine for large-scale data processing",
            "Apache Flink is an open-source stream processing framework",
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
