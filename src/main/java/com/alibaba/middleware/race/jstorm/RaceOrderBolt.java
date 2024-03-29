package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairManageFactory;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderStream;
import com.alibaba.middleware.race.model.PaymentStream;
import com.sunny.utils.CommonShareData;
import com.sunny.utils.OperateFileOld;
import com.taobao.tair.impl.DefaultTairManager;

public class RaceOrderBolt implements IRichBolt{
	Logger LOG = LoggerFactory.getLogger(RaceOrderBolt.class); 
	OutputCollector collector;
	DefaultTairManager tairManager;
	TairOperatorImpl tairOP;
	LinkedBlockingQueue<PaymentStream> paymentQueue = null;
	AtomicInteger orderNum = new AtomicInteger(0);
	AtomicInteger paymentNum = new AtomicInteger(0);
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("orderId","Order"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
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
		OrderStream orderStream = (OrderStream)obj;
		this.collector.emit(new Values(orderStream.getOrderId(),orderStream));
		this.collector.ack(input);
		
	}
	@Override
	public void cleanup() {
		//OperateFileOld.writeToFile("orderNum:"+orderNum+",paymentNUM:"+paymentNum);
	}

}
