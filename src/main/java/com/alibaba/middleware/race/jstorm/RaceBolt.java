package com.alibaba.middleware.race.jstorm;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.model.OrderStream;
import com.alibaba.middleware.race.model.PaymentStream;

public class RaceBolt implements IRichBolt{
	Logger LOG = LoggerFactory.getLogger(RaceBolt.class); 
	OutputCollector collector;
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("receivemessage"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		
		this.collector = collector;
		
	}

	@Override
	public void execute(Tuple input) {
		
		Object obj = input.getValue(0);
		
		if(obj instanceof OrderStream){
			OrderStream orderStream = (OrderStream)obj;
			LOG.info("panzha:receive from spout:"+input.getMessageId().toString()+" "+orderStream.toString());
		}
		if(obj instanceof PaymentStream){
			PaymentStream payStream = (PaymentStream)obj;
			LOG.info("panzha:receive from spout:"+input.getMessageId().toString()+" "+payStream.toString());
		}
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}
