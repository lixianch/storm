package bolts;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lixianch on 2016/9/10.
 */
public class WordCounter extends BaseBasicBolt {
    private Integer id;
    private String name;
    private Map<String,Integer> counters;
    @Override
    public void cleanup() {
        System.out.println("-- Word Counter ["+name+"-"+id+"] --");
        for(Map.Entry<String, Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.id = context.getThisTaskId();
        this.name = context.getThisComponentId();
        this.counters = new HashMap<String, Integer>();
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getString(0);
        if(!counters.containsKey(word)){
            counters.put(word, 1);
        }else {
            Integer count = counters.get(word);
            counters.put(word, count + 1);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
