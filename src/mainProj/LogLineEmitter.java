package mainProj;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class LogLineEmitter extends BaseRichSpout{
   BufferedReader bufferedReader = null;
   SpoutOutputCollector collector;
   private int field;
   
   public LogLineEmitter(int field) {
      this.field = field;
   }
   
   @Override
   public void open(Map conf, TopologyContext context,
         SpoutOutputCollector collector) {
      prepare();
      this.collector = collector;
      
      
   }
   
   private void prepare() {
      try {
         File f = new File("access.log");
         bufferedReader = new BufferedReader(new FileReader(f));
      } catch (FileNotFoundException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

   @Override
   public void nextTuple() {
      //Utils.sleep();
      String outputLine;
      outputLine = "";
      try {
         if ((outputLine = bufferedReader.readLine()) != null) {
            Object[] logArray = outputLine.split(" ");
            collector.emit(new Values(logArray[field]));
         } else {
            //start over
            bufferedReader.close();
            prepare();
         }
      } catch (IOException e) {
         // TODO Auto-generated catch block
         e.printStackTrace();
      }
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("log line"));
   }
   
}
