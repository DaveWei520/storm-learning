package com.gome.bigData.demo.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author weiyu@gomeholdings.com
 * @description
 * @create 2017/6/28
 */
public class SentenceSpout implements IRichSpout {

    private SpoutOutputCollector collector;
    private static final String[] sentences = {"The logic for a realtime application is packaged into a Storm topology",
                        "A Storm topology is analogous to a MapReduce job",
                         "One key difference is that a MapReduce job eventually finishes whereas a topology runs forever",
                         " A topology is a graph of spouts and bolts that are connected with stream groupings"};
    private int index = 0;

    //初始化操作
    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    //核心逻辑
    @Override
    public void nextTuple() {
        collector.emit(new Values(sentences[index]));
        index++;
        if(index >= sentences.length){
            index=0;
        }
    }

    @Override
    public void ack(Object msgId) {

    }

    @Override
    public void fail(Object msgId) {

    }

    //向下游输出
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("sentences"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
