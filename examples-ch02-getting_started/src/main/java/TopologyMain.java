import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.WordCounter;
import bolts.WordNormalizer;
import spouts.WordReader;

/**
 * Created by lixianch on 2016/9/10.
 */
public class TopologyMain {
    public static void main(String[] args) throws InterruptedException{
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader());
        builder.setBolt("word-normalizer", new WordNormalizer()).shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),1).fieldsGrouping("word-normalizer", new Fields("word"));
        Config config = new Config();
        config.put("wordsFile", args[0]);
        config.setDebug(false);

        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Getting Started Topology",config, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
