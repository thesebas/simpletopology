package giga.cockpit.storm;

import backtype.storm.Config;
import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

import java.util.Map;


public class MyDRPCClient {

    public static void main(String[] args) throws Exception {

        Map defaults = backtype.storm.utils.Utils.findAndReadConfigFile("defaults.yaml");
        Config conf = new Config();
        conf.putAll(defaults);

        conf.setDebug(true);
        conf.put(Config.TOPOLOGY_DEBUG, false);

        DRPCClient client = new DRPCClient(conf, "localhost", 3772);

        try {
            String result = client.execute("count-pis", "channel:software,channel:apple");
            System.out.println(result);
        } catch (DRPCExecutionException e) {
            System.out.println(e.getMessage());
        }
    }
}
