package giga.cockpit.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class FBBolt extends BaseRichBolt {

    private static final Logger LOG = LoggerFactory.getLogger(FBBolt.class);

    OutputCollector collector;
    Random random;
    TopologyContext topologyContext;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        random = new Random();
//        LOG.info("prepare: taskId {}, index: {}, config: {}", topologyContext.getThisTaskId(), topologyContext.getThisTaskIndex(), map);
        this.topologyContext = topologyContext;
    }

    @Override
    public void execute(Tuple tuple) {

        String url = tuple.getStringByField("url");
        LOG.info("url: {}, idx: {}, id: {}", url, topologyContext.getThisTaskIndex(), topologyContext.getThisTaskId());
        collector.emit(new Values(url, random.nextInt(100), 100 + random.nextInt(100)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("url", "comments", "likes"));
    }
}
