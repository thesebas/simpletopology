package giga.cockpit.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import giga.cockpit.storm.spout.RedisSubSpout;

public class CockpitTopology {


    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("from-redis", new RedisSubSpout(), 10);

        builder.setBolt("fb-bolt", new FBBolt(), 3)
                .shuffleGrouping("from-redis");
        builder.setBolt("ga-bolt", new GABolt(), 3)
                .shuffleGrouping("from-redis");
        builder.setBolt("abs-bolt", new AbsBolt(), 3)
                .shuffleGrouping("from-redis");

        builder.setBolt("report-bolt", new ReportBolt(), 1)
                .globalGrouping("fb-bolt")
                .globalGrouping("ga-bolt")
                .globalGrouping("abs-bolt");


        // create the default config object
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);

        if (args != null && args.length > 0) {

            // run it in a live cluster

            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(3);

            // create the topology and submit with config
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());

        } else {

            // run it in a simulated local cluster

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("simple", conf, builder.createTopology());

            // let the topology run for 30 seconds. note topologies never terminate!
            Thread.sleep(60000);

            // kill the topology
            cluster.killTopology("simple");

            // we are done, so shutdown the local cluster
            cluster.shutdown();
        }
    }
}
