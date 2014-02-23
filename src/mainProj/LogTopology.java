package mainProj;


import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class LogTopology {
   
   
   public static void main(String[] args) throws Exception {
      TopologyBuilder builder = new TopologyBuilder();
      
      
      builder.setSpout("spoutput", new LogLineEmitter(0), 1);
      builder.setBolt("bolt1", new CounterBolt("newoutput.txt"), 1).shuffleGrouping("spoutput");
      builder.setSpout("spoutput2", new LogLineEmitter(8), 1);
      builder.setBolt("bolt2", new CounterBolt("newoutput2.txt"), 1).shuffleGrouping("spoutput2");

      Config conf = new Config();
      conf.setDebug(true);
      
      if (args != null && args.length > 0) {
         conf.setNumWorkers(3);

         StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
       }
       else {

         LocalCluster cluster = new LocalCluster();
         cluster.submitTopology("test", conf, builder.createTopology());
         Utils.sleep(100000);
         cluster.killTopology("test");
         cluster.shutdown();
       }
      
      
      
   }
}
