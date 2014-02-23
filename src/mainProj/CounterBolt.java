package mainProj;


import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.security.KeyStore.Entry;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.hooks.info.EmitInfo;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * Counts the number of occurences of requests from IP addresses
 * @author websterm
 */
public class CounterBolt extends BaseRichBolt {
   OutputCollector _collector;
   private int counter;
   private HashMap<String, Integer> map = new HashMap<String, Integer>();
   private PrintWriter fw = null;
   //write every 100 records
   private static final int WRITE_PERIOD = 100;
   private String outputName;

   public CounterBolt(String outputName) {
      this.outputName = outputName;
   }

   @Override
   public void prepare(Map stormConf, TopologyContext context,
         OutputCollector collector) {
      counter = 0;
      _collector = collector;



   }

   @Override
   public void execute(Tuple input) {
      String ip = input.getString(0);
      if (map.containsKey(ip)) {
         //if the map contains the key, increment the count for it
         map.put(ip, map.get(ip) + 1);
      } else {
         //otherwise, add it to the array
         map.put(ip, 1);
      }

      counter++;
      /* when counter reaches 10 records, write to file */
      if (counter > WRITE_PERIOD) {
         counter = 0;
         writeToFile(map);
      }

      _collector.emit(new Values(ip, map.get(ip)));
   }

   @Override
   public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("ip", "count"));
   }

   public HashMap<String, Integer> getFrequencyMap() {
      return map;
   }

   public void writeToFile(HashMap<String, Integer> map) {
      try {
         fw = new PrintWriter(outputName, "UTF-8");
      } catch (FileNotFoundException e) {
         e.printStackTrace();
      } catch (UnsupportedEncodingException e) {
         e.printStackTrace();
      }
      Set<Map.Entry<String, Integer>> pairs = map.entrySet();
      // put all the entries in the map into a set
      List<Map.Entry<String, Integer>> pairList = 
            new ArrayList<Map.Entry<String, Integer>>();
      pairList.addAll(pairs);
      //sort the set
      Collections.sort(pairList, new Comparator<Map.Entry<String, Integer>>() {
         //in line comparator (reverse order sort)
         @Override
         public int compare(java.util.Map.Entry<String, Integer> o1,
               java.util.Map.Entry<String, Integer> o2) {
            return o2.getValue() - o1.getValue();
         }

      });

      // print the set
      for (Map.Entry<String, Integer> entry : pairList) {
         fw.println(entry.getKey() + ": " + entry.getValue());
      }
      if (fw != null) {
         fw.close();
      }
   }



}
