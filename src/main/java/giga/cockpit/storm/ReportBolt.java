package giga.cockpit.storm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.*;

import com.google.gson.*;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bolt that prints the word and count to redis
 */


public class ReportBolt extends BaseRichBolt {
    private static final Logger LOG = LoggerFactory.getLogger(ReportBolt.class);

    transient RedisConnection<String, String> redis;

    protected Map<String, Map<String, String>> store;

    TopologyContext topologyContext;
    protected Gson gson;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

        redis = RedisClient.create("redis://127.0.0.1:6379/0").connect();
        store = new HashMap<>();
        gson = new Gson();
        this.topologyContext = topologyContext;

    }

    @Override
    public void execute(Tuple tuple) {
//    "fb-bolt"
//    "ga-bolt"
//    "abs-bolt"

        String url = tuple.getStringByField("url");
        LOG.info("input from {} for url {} processed in {}", tuple.getSourceComponent(), url, topologyContext.getThisTaskId());

        switch (tuple.getSourceComponent()) {
            case "fb-bolt":
                processFB(url, tuple);
                break;
            case "ga-bolt":
                processGA(url, tuple);
                break;
            case "abs-bolt":
                processABS(url, tuple);
                break;
        }
        Map<String, String> allDataWeHave = getAllDataAvailable(url);
        if (checkIfAllDataAvailable(allDataWeHave)) {
            LOG.info("go all data for [{}], publising", url);
            HashMap<String, String> msg = new HashMap<>(allDataWeHave);
            msg.put("url", url);
            redis.publish("URLSenriched", gson.toJson(msg));
        }
    }

    private boolean checkIfAllDataAvailable(Map<String, String> map) {
        return map.keySet().containsAll(Arrays.asList("pi", "comments", "likes", "revenue"));
    }

    private Map<String, String> getAllDataAvailable(String url) {
        return getStoreFor(url);
    }

    protected void processGA(String url, Tuple tuple) {
        Map<String, String> data = getStoreFor(url);
        data.put("pi", tuple.getIntegerByField("pi").toString());
    }

    protected void processFB(String url, Tuple tuple) {
        Map<String, String> data = getStoreFor(url);
//        "url", "comments", "likes"

        data.put("comments", tuple.getIntegerByField("comments").toString());
        data.put("likes", tuple.getIntegerByField("likes").toString());
    }

    protected void processABS(String url, Tuple tuple) {
        Map<String, String> data = getStoreFor(url);
//        "url", "revenue"
        data.put("revenue", tuple.getFloatByField("revenue").toString());
    }


    protected Map<String, String> getStoreFor(String url) {
        if (!store.containsKey(url)) {
            Map<String, String> map = new HashMap<>();
            store.put(url, map);
            return map;
        }
        return store.get(url);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to add - since it is the final bolt
    }
}
