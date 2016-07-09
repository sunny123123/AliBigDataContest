package com.sunny.backup;

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

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairManageFactory;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.model.OrderStream;
import com.alibaba.middleware.race.model.PaymentStream;
import com.sunny.utils.CommonShareData;
import com.sunny.utils.OperateFileOld;
import com.taobao.tair.impl.DefaultTairManager;

public class RaceMergeBolt implements IRichBolt{
	Logger LOG = LoggerFactory.getLogger(RaceMergeBolt.class); 
	OutputCollector collector;
	DefaultTairManager tairManager;
	TairOperatorImpl tairOP;
	LinkedBlockingQueue<PaymentStream> paymentQueue = null;
	AtomicInteger orderNum = new AtomicInteger(0);
	AtomicInteger paymentNum = new AtomicInteger(0);
	class QueueThread implements Runnable{
		@Override
		public void run() {
			while(true){
				PaymentStream pay = null ;
				//Thread.currentThread().sleep(1);
				LOG.info("panzha:queueNUM"+paymentQueue.size());
				pay = paymentQueue.poll();
				//OperateFile.writeToFile("put "+pay+" to queue");
				if(pay!=null)
					addTradAmountToTair(pay);
			}
			
		}
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("orderId","RECORD"));
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
		//tairManager = TairManageFactory.getDefaultTairManager();
		this.tairOP = new TairOperatorImpl();
		//this.paymentQueue = new LinkedBlockingQueue<PaymentStream>();
		this.paymentQueue = CommonShareData.getBoltPaymentBlockingQueue();
		Thread t = new Thread(new QueueThread());
		OperateFileOld.writeToFile("queue thread start");
		t.start();
	}

	@Override
	public void execute(Tuple input) {
		
		long orderId = (long)input.getValue(0);
		Object record = input.getValue(1);
		if(input.getSourceComponent().equals("MetaOrderSpout")){
	
			OrderStream orderStream = (OrderStream)record;
			LOG.info("panzha:receive from spout:Order"+input.getMessageId().toString()+" "+orderStream.toString());
			tairOP.putOrder(RaceConfig.TairNamespace, orderStream.getOrderId(), orderStream.getType());
			orderNum.getAndIncrement();
		}
		if(input.getSourceComponent().equals("MetaPaymentSpout")){
			PaymentStream payStream = (PaymentStream)record;
			LOG.info("panzha:receive from spout:Pay"+input.getMessageId().toString()+" "+payStream.toString());
			/*write tmall and taobao trade amount every minutes*/
			addTradAmountToTair(payStream);
			paymentNum.getAndIncrement();
			/*deal with pc and wireless something*/
		}
		//this.collector.ack(input);
		
	}
	/*add tamll and taobao trade amount to tair*/
	public void addTradAmountToTair(PaymentStream payStream){
		String rs = tairOP.getOrderRs(RaceConfig.TairNamespace, payStream.getOrderId());
		if(!rs.equals("-1")){
			if(rs.equals("TMALL")){
				tairOP.updateDataTotair(RaceConfig.TairNamespace, RaceConfig.prex_tmall+payStream.getCreateTime(),
						payStream.getPayAmount());
			}
			if(rs.equals("TAOBAO")){
				tairOP.updateDataTotair(RaceConfig.TairNamespace, RaceConfig.prex_taobao+payStream.getCreateTime(),
						payStream.getPayAmount());
			}
		}
		else{
			LOG.info("panzha:getOrderTair failed,add to queue");
			try {
				paymentQueue.put(payStream);//this payment message cannot find order in tair,and add this payment to queue
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		OperateFileOld.writeToFile("orderNum:"+orderNum+",paymentNUM:"+paymentNum);
	}

}
