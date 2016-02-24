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

/**
 * A bolt that prints the word and count to redis
 */
public class ReportBolt extends BaseRichBolt {
    transient RedisConnection<String, String> redis;

    @Override
    public void prepare(
            Map map,
            TopologyContext topologyContext,
            OutputCollector outputCollector) {
        RedisClient client = RedisClient.create("redis://127.0.0.1:6379/0");

        redis = client.connect();
    }

    @Override
    public void execute(Tuple tuple) {
//    "fb-bolt"
//    "ga-bolt"
//    "abs-bolt"
        String url = tuple.getStringByField("url");
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
        Map<String,String> allDataWeHave = getAllDataAvailable(url);
        if (checkIfAllDataAvailable(allDataWeHave)) {
            Gson gson = new GsonBuilder().create();
            allDataWeHave.put("url", url);
            redis.publish("URLSenriched", gson.toJson(allDataWeHave));
        }
    }

    private boolean checkIfAllDataAvailable(Map<String,String> map) {
        return map.keySet().containsAll(Arrays.asList("pi", "comments", "likes", "revenue"));
    }
    private Map<String,String> getAllDataAvailable(String url) {
        return redis.hgetall(url);
    }

    protected void processGA(String url, Tuple tuple) {
        Map<String, String> data = new HashMap<>();
        data.put("pi", tuple.getIntegerByField("pi").toString());
        redis.hmset(url, data);
    }

    protected void processFB(String url, Tuple tuple) {
        Map<String, String> data = new HashMap<>();
//        "url", "comments", "likes"
        data.put("comments", tuple.getIntegerByField("comments").toString());
        data.put("likes", tuple.getIntegerByField("likes").toString());
        redis.hmset(url, data);
    }

    protected void processABS(String url, Tuple tuple) {
        Map<String, String> data = new HashMap<>();
//        "url", "revenue"
        data.put("revenue", tuple.getFloatByField("revenue").toString());
        redis.hmset(url, data);
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // nothing to add - since it is the final bolt
    }
}
