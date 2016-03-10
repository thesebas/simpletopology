package giga.cockpit.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import storm.trident.Stream;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.*;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.testing.MemoryMapState;
import storm.trident.tuple.TridentTuple;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Created by thesebas on 2016-03-09.
 */
public class GroupCountTopology {
    public static void main(String[] args) throws Exception {


        TridentTopology topology = new TridentTopology();

        String query = "SELECT \"url\", \"date\", \"pi\", \"pif\", \"tags\" FROM \"cockpit2_testTogether\"";
        Stream stream = topology.newStream("cassandra", new CassandraSpout(query))
                .each(
                        new Fields("date", "tags", "pi", "pif"),
                        new TagExpander(),
                        new Fields("ndate", "ntag", "npi", "pnif")
                );

        TridentState state = stream
                .project(new Fields("ntag", "npi"))
                .groupBy(new Fields("ntag"))
                .persistentAggregate(
                        new MemoryMapState.Factory(),
                        new Sum(),
                        new Fields("pis")
                );


        topology.newDRPCStream("count-pis")
                .each(new Fields("args"),
                        new SplitOnDelimiter(","),
                        new Fields("tag"))
                .groupBy(new Fields("tag"))
                .stateQuery(state,
                        new Fields("tag"),
                        new MapGet(),
                        new Fields("count"));
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, topology.build());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("grouping", conf, topology.build());
            Thread.sleep(60000);
            cluster.killTopology("grouping");
            cluster.shutdown();
        }
    }

    private static class TagExpander extends BaseFunction {
        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            Set<String> tags = (Set<String>) tridentTuple.getValueByField("tags");
            for (String tag : tags) {
                tridentCollector.emit(new Values(
                        tridentTuple.getValueByField("date"),
                        tag,
                        tridentTuple.getValueByField("pi"),
                        tridentTuple.getValueByField("pif")
                ));
            }
        }
    }

    private static class CassandraSpout extends BaseRichSpout {
        private final String query;
        transient private Cluster cluster;
        transient private ResultSet rs;
        transient private Iterator<Row> rsiterator;
        private SpoutOutputCollector outputCollector;


        public CassandraSpout(String query) {
            this.query = query;



        }

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            cluster = Cluster.builder()
                    .addContactPoint("10.0.40.42")
                    .build();

            outputCollector = collector;

            Session session = cluster.connect("el_test");

            rs = session.execute(this.query);
            rsiterator = rs.iterator();

        }

        @Override
        public void nextTuple() {
            if (rsiterator.hasNext()) {
                Row row = rsiterator.next();

                outputCollector.emit(new Values(
                        row.getString("url"),
                        row.getTimestamp("date"),
                        row.getInt("pi"),
                        row.getFloat("pif"),
                        row.getSet("tags", String.class)
                ));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("url", "date", "pi", "pif", "tags"));
        }

        @Override
        public Map<String, Object> getComponentConfiguration() {
            Config ret = new Config();
            ret.setMaxTaskParallelism(1);//force one spout process
            return ret;
        }
    }

    private static class PiCounter implements CombinerAggregator {
        @Override
        public Object init(TridentTuple tridentTuple) {
            return null;
        }

        @Override
        public Object combine(Object o, Object t1) {
            return null;
        }

        @Override
        public Object zero() {
            return null;
        }
    }

    private static class SplitOnDelimiter extends BaseFunction {

        private final String s;

        public SplitOnDelimiter(String s) {
            this.s = s;
        }

        @Override
        public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
            for (String arg : tridentTuple.getString(0).split(this.s)) {
                tridentCollector.emit(new Values(arg.trim()));
            }

        }
    }
}