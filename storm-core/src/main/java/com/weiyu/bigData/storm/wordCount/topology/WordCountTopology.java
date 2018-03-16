package com.weiyu.bigData.storm.wordCount.topology;

import com.weiyu.bigData.storm.wordCount.bolt.ReportBolt;
import com.weiyu.bigData.storm.wordCount.bolt.SplitSentenceBolt;
import com.weiyu.bigData.storm.wordCount.bolt.WordCountBolt;
import com.weiyu.bigData.storm.wordCount.spout.SentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author weiyu@gomeholdings.com
 * @description
 * @create 2017/6/27
 */
public class WordCountTopology {
    private static final Logger log = LoggerFactory.getLogger(WordCountTopology.class);

    //各个组件名字的唯一标识
    private static final String SENTENCE_SPOUT_ID = "sentence-spout";
    private static final String SPLIT_SENTENCE_BOLT_ID = "split-bolt";
    private static final String WORD_COUNT_BOLT_ID = "count-bolt";
    private static final String REPORT_BOLT_ID = "report-bolt";

    //拓扑名称
    private static final String TOPOLOGY_NAME = "word-count-topology";

    public static void main(String[] args) {
        log.info(".........begining.......");
        //各组件的实例
        SentenceSpout sentenceSpout = new SentenceSpout();
        SplitSentenceBolt splitSentenceBolt = new SplitSentenceBolt();
        WordCountBolt wordCountBolt = new WordCountBolt();
        ReportBolt reportBolt = new ReportBolt();

        //构建一个拓扑
        TopologyBuilder builder = new TopologyBuilder();
        //配置第一个组件sentenceSpout
        builder.setSpout(SENTENCE_SPOUT_ID, sentenceSpout);
        //配置第二个组件splitSentenceBolt，上游为sentenceSpout，tuple分组方式为随机分组shuffleGrouping
        builder.setBolt(SPLIT_SENTENCE_BOLT_ID, splitSentenceBolt).shuffleGrouping(SENTENCE_SPOUT_ID);
        //配置第三个组件wordCountBolt，上游为splitSentenceBolt，tuple分组方式为fieldsGrouping，同一个单词进入同一个task中（bolt实例）
        builder.setBolt(WORD_COUNT_BOLT_ID, wordCountBolt).fieldsGrouping(SPLIT_SENTENCE_BOLT_ID, new Fields("word"));
        //配置最后一个组件reportBolt，上游为wordCountBolt，tuple分组方式为globalGrouping，即所有的tuple都进入一个task中
        builder.setBolt(REPORT_BOLT_ID, reportBolt).globalGrouping(WORD_COUNT_BOLT_ID);

        //配置
        Config config = new Config();
        /*建立本地集群,利用LocalCluster,storm在程序启动时会在本地自动建立一个集群,不需要用户自己再搭建,方便本地开发和debug*/
        LocalCluster cluster = new LocalCluster();
        //建立拓扑实例，并提交到本地集群运行
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        //集群下使用StormSubmitter提交拓扑
        //StormSubmitter.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());
    }
}
