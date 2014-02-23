package mainProj;


import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class IdentityBolt extends BaseRichBolt {

   private static final long serialVersionUID = 1L;
   OutputCollector _collector;
   
   
   @Override
   public void prepare(Map stormConf, TopologyContext context,
         OutputCollector collector) {
      _collector = collector;
   }

   @Override
   public void execute(Tuple input) {
      _collector.emit(input, new Values(input));
      _collector.ack(input);
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("line"));
      
   }
   
   
}
