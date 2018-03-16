package com.weiyu.bigData.storm.wordCount.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author weiyu@gomeholdings.com
 * @description
 * @create 2017/6/28
 */
public class WordCountBolt extends BaseRichBolt {
    private OutputCollector collector;
    //保存单词计数
    private Map<String, Long> wordCount = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        wordCount = new ConcurrentHashMap<>();
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getStringByField("word");
        Long count = wordCount.get(word);
        if(count == null){
            count = 0L;
        }
        count++;
        wordCount.put(word,count);
        collector.emit(new Values(word,count));
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word","count"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
