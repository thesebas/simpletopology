package giga.cockpit.storm.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubConnection;
import com.lambdaworks.redis.pubsub.RedisPubSubListener;


import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;


public class RedisSubSpout extends BaseRichSpout {
    SpoutOutputCollector collector;

    RedisConnection<String, String> redis;
    RedisPubSubConnection<String, String> pubSub;
    LinkedBlockingQueue<String> queue;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        // instantiate a redis connection
        RedisClient client = new RedisClient("127.0.0.1", 6379);

        // initiate the actual connection
        redis = client.connect();
        pubSub = client.connectPubSub();

        String key = "URLS";

        queue = new LinkedBlockingQueue<>();

        Thread t = new PubSubThread(pubSub, queue, key);
        t.run();

    }

    @Override
    public void nextTuple() {
        String msg = queue.poll();

        JsonObject obj = new JsonParser().parse(msg).getAsJsonObject();
        collector.emit(new Values(
                obj.get("url").getAsString(),
                obj.get("author").getAsString(),
                obj.get("resort").getAsString(),
                obj.get("factor").getAsString()
        ));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "author", "resort", "factor"));
    }

    class PubSubThread extends Thread {

        RedisPubSubConnection<String, String> pubSub;
        LinkedBlockingQueue<String> queue;
        String key;

        public PubSubThread(RedisPubSubConnection<String, String> pubSub, LinkedBlockingQueue<String> queue, String key) {
            this.pubSub = pubSub;
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
                    queue.offer(message);
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
        }
    }
}
