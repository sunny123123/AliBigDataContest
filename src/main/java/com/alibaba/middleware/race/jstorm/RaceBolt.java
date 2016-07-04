package com.alibaba.middleware.race.jstorm;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

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
import com.sunny.utils.OperateFile;
import com.taobao.tair.impl.DefaultTairManager;

public class RaceBolt implements IRichBolt{
	Logger LOG = LoggerFactory.getLogger(RaceBolt.class); 
	OutputCollector collector;
	DefaultTairManager tairManager;
	TairOperatorImpl tairOP;
	LinkedBlockingQueue<PaymentStream> paymentQueue;
	
	class QueueThread implements Runnable{
		@Override
		public void run() {
			while(true){
				PaymentStream pay = null ;
				try {
					//Thread.currentThread().sleep(500);
					LOG.info("panzha:queueNUM"+paymentQueue.size());
					pay = paymentQueue.take();
					OperateFile.writeToFile("put "+pay+" to queue");
					addTradAmountToTair(pay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
	}
	
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
		//tairManager = TairManageFactory.getDefaultTairManager();
		this.tairOP = new TairOperatorImpl();
		this.paymentQueue = new LinkedBlockingQueue<PaymentStream>();
		Thread t = new Thread(new QueueThread());
		OperateFile.writeToFile("thread start");
		t.start();
	}

	@Override
	public void execute(Tuple input) {
		
		Object obj = input.getValue(0);
		
		if(obj instanceof OrderStream){
			OrderStream orderStream = (OrderStream)obj;
			LOG.info("panzha:receive from spout:"+input.getMessageId().toString()+" "+orderStream.toString());
			tairOP.putOrder(RaceConfig.TairNamespace, orderStream.getOrderId(), orderStream.getType());
		}
		if(obj instanceof PaymentStream){
			PaymentStream payStream = (PaymentStream)obj;
			LOG.info("panzha:receive from spout:"+input.getMessageId().toString()+" "+payStream.toString());
			/*write tmall and taobao trade amount every minutes*/
			addTradAmountToTair(payStream);
			
			/*deal with pc and wireless something*/
		}
		
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
		
	}

}
