package spouts;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

/**
 * Created by lixianch on 2016/9/9.
 */
public class WordReader extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private FileReader fileReader;
    private boolean completed = false;

    @Override
    public void close() {
    }

    @Override
    public void ack(Object msgId) {
        System.out.println("OK:" + msgId);
    }

    @Override
    public void fail(Object msgId) {
        System.out.println("FAIL:" + msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    @Override
    public void open(Map conf, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try {
            this.fileReader = new FileReader(conf.get("wordsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("wordFile")+"]");
        }
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        if(completed){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                //Do nothing
            }
            return;
        }

        String str;
        BufferedReader bufferedReader = new BufferedReader(fileReader);
        try {
            while((str = bufferedReader.readLine()) != null){
                this.collector.emit(new Values(str),str);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading tuple",e);
        } finally {
            completed = true;
        }
    }
}
