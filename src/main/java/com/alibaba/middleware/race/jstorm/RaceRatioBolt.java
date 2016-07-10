package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.TreeMap;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.model.PaymentStream;
import com.alibaba.middleware.race.model.RatioStream;

public class RaceRatioBolt implements IRichBolt{
	
	OutputCollector collector;
	TreeMap<Long, RatioStream> map = new TreeMap<Long, RatioStream>();
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		Object obj = input.getValue(0);
		PaymentStream payStream = (PaymentStream)obj;
		if(payStream.getOrderId()!=-1111L){
			
			Long key = payStream.getCreateTime();
			if(payStream.getPayPlatform()==1){//wire
				if(map.containsKey(key)){
					RatioStream ratio = map.get(key);
					ratio.setWireMoney(ratio.getWireMoney()+payStream.getPayAmount());
					map.put(key,ratio);
				}
				else
					map.put(key, new RatioStream(0.0,payStream.getPayAmount()));
			}
			else{//pc
				if(map.containsKey(key)){
					RatioStream ratio = map.get(key);
					ratio.setPcMoney(ratio.getPcMoney()+payStream.getPayAmount());
					map.put(key,ratio);
				}
				else
					map.put(key, new RatioStream(0.0,payStream.getPayAmount()));
			}
				
		}
		
		this.collector.ack(input);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
