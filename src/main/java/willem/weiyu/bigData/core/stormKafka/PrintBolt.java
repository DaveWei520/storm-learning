package willem.weiyu.bigData.core.stormKafka;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @Author weiyu005
 * @Description
 * @Date 2018/11/1 17:30
 */
public class PrintBolt extends BaseBasicBolt {

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Fields fields = input.getFields();
        Iterator<String> fieldItr = fields.iterator();
        Map params = new HashMap<>();
        while (fieldItr.hasNext()){
            String key = fieldItr.next();
            params.put(key,input.getValueByField(key));
        }
        System.out.println("******kafka data:"+params+"******");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
