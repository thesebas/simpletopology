package giga.cockpit.storm.spout;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class RedisSubSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSubSpout.class);

    SpoutOutputCollector collector;

    RedisPubSubConnection<String, String> pubSubConnection;
    RedisConnection<String, String> dataRedis;
    LinkedBlockingQueue<String> queue;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        pubSubConnection = RedisClient.create("redis://127.0.0.1:6379/0").connectPubSub();
        dataRedis = RedisClient.create("redis://127.0.0.1:6379/0").connect();

        String key = "URLS";

        queue = new LinkedBlockingQueue<>(100);
        Thread t = new PubSubThread(pubSubConnection, dataRedis, queue, key);
        t.start();

    }

    @Override
    public void nextTuple() {
        String msg = queue.poll();
        if (msg == null) {
            Utils.sleep(50);
        } else {
            JsonObject obj = new JsonParser().parse(msg).getAsJsonObject();
            collector.emit(new Values(
                    obj.get("url").getAsString(),
                    obj.get("author").getAsString(),
                    obj.get("resort").getAsString(),
                    obj.get("factor").getAsFloat()
            ));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "author", "resort", "factor"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Config ret = new Config();
        ret.setMaxTaskParallelism(1);//force one spout process
        return ret;
    }

    class PubSubThread extends Thread {

        RedisPubSubConnection<String, String> pubSub;
        RedisConnection<String, String> data;
        LinkedBlockingQueue<String> queue;
        String key;

        public PubSubThread(RedisPubSubConnection<String, String> pubSub, RedisConnection<String, String> data, LinkedBlockingQueue<String> queue, String key) {
            this.pubSub = pubSub;
            this.data = data;
            this.queue = queue;
            this.key = key;
        }

        public void run() {
            pubSub.addListener(new RedisPubSubListener<String, String>() {
                @Override
                public void message(String channel, String message) {
                    queue.offer(message);
                }

                @Override
                public void message(String pattern, String channel, String message) {
                }

                @Override
                public void subscribed(String s, long l) {

                }

                @Override
                public void psubscribed(String s, long l) {

                }

                @Override
                public void unsubscribed(String s, long l) {

                }

                @Override
                public void punsubscribed(String s, long l) {

                }
            });
            pubSub.subscribe(key);
        }
    }
}
