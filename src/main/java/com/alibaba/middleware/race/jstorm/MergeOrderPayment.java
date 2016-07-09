package com.alibaba.middleware.race.jstorm;

import java.io.BufferedWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairManageFactory;
import com.alibaba.middleware.race.model.OrderStream;
import com.alibaba.middleware.race.model.PaymentStream;
import com.sunny.utils.OperateFile;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.impl.DefaultTairManager;

public class MergeOrderPayment implements IRichBolt {
	private Logger LOG = LoggerFactory.getLogger(MergeOrderPayment.class);
	private OutputCollector collector = null;
	private LinkedBlockingQueue<PaymentStream> paymentQueue = null;
	BufferedWriter mergeLog = null;
	Map<Long, String> orderMap = null;
	AtomicInteger payDealNum = new AtomicInteger(0);
	Set<String> rsKey = null;
	BufferedWriter tairLog = null;
	BufferedWriter rsLog = null;
	ConcurrentHashMap<String, Double> payRs = new ConcurrentHashMap<String, Double>();
	
	//Map<String, Double> payRs = new HashMap<String,Double>();
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		orderMap = new HashMap<Long, String>();
		paymentQueue = new LinkedBlockingQueue<PaymentStream>();
		rsKey = new HashSet<String>();
		if(RaceConfig.LogFlag){
			mergeLog = OperateFile.getWriter(context.getThisComponentId().toLowerCase()+ "_"
					+ context.getThisTaskId());
			tairLog = OperateFile.getWriter(context.getThisComponentId().toLowerCase()+ "_"
					+ context.getThisTaskId()+"_tairLog");
			rsLog = OperateFile.getWriter(context.getThisComponentId().toLowerCase()+ "_"
					+ context.getThisTaskId()+"_rsLog");
			OperateFile.writeContent(mergeLog, "merge thread start");
		}
		ExecutorService service = Executors.newFixedThreadPool(2);
		service.submit(new QueueThread());
		service.submit(new WriteRsToTair());
		
	}

	@Override
	public void execute(Tuple input) {
		long orderId = (long) input.getValue(0);
		Object record = input.getValue(1);

		if (input.getSourceComponent().equals("OrderBolt")) {
			OrderStream orderStream = (OrderStream) record;
			orderMap.put(orderStream.getOrderId(), orderStream.getType());
		}

		if (input.getSourceComponent().equals("PaymentBolt")) {
			PaymentStream payStream = (PaymentStream) record;
			try {
				paymentQueue.put(payStream);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		this.collector.ack(input);
	}

	@Override
	public void cleanup() {
		if(RaceConfig.LogFlag){
			OperateFile.writeContent(mergeLog, "payment queue remained size:"
					+ this.paymentQueue.size());
			OperateFile.writeContent(mergeLog, "total deal entries:"
					+ this.payDealNum);
			Iterator<String> it = rsKey.iterator();
			while(it.hasNext()){
				String key = it.next();
				DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
				Result<DataEntry> rs = tairManager.get(RaceConfig.TairNamespace, key);
				OperateFile.writeContent(tairLog, key+":"+rs.getValue());
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	class QueueThread implements Runnable {
		@Override
		public void run() {
			while (true) {

				PaymentStream payStream = null;
				try {
					payStream = paymentQueue.take();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
				String type = orderMap.get(payStream.getOrderId());
				if (type == null) {
					try {
						paymentQueue.put(payStream);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				} else {
					payStream.setType(type);
					//collector.emit(new Values(payStream));
					/**
					 * write middle result to Map
					 */
					writePaytoRsMap(payStream);
					
					if(RaceConfig.LogFlag){
						OperateFile.writeContent(mergeLog,
								"mergerOderPayment bolt receive:"+ payStream.toString());
						
						
						
						payDealNum.getAndIncrement();
					}
				}
				// OperateFile.writeToFile("put "+pay+" to queue");
			}
		}
		public void writePaytoRsMap(PaymentStream pay){
			if(pay.getType().equals("TAOBAO")){
				String preTaobao = RaceConfig.prex_taobao+pay.getCreateTime();
				if(payRs.containsKey(preTaobao)){
					payRs.put(preTaobao, payRs.get(preTaobao)+pay.getPayAmount());
				}
				else{
					payRs.put(preTaobao, pay.getPayAmount());
				}
				rsKey.add(preTaobao);
				//System.out.println("a:"+payRs.get(preTaobao));
			}
			if(pay.getType().equals("TMALL")){
				String preTmall = RaceConfig.prex_tmall+pay.getCreateTime();
				if(payRs.containsKey(preTmall)){
					payRs.put(preTmall, payRs.get(preTmall)+pay.getPayAmount());
				}
				else{
					payRs.put(preTmall, pay.getPayAmount());
				}
				rsKey.add(preTmall);
				//System.out.println("b:"+payRs.get(preTmall));
			}
			
		}
	}
	
	/*
	 * write result to tair every 10s
	 */
	class WriteRsToTair implements Runnable{

		@Override
		public void run() {
//			try {
//				Thread.sleep(1*1000);
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
			Iterator<String> it = payRs.keySet().iterator();
			//System.out.println("panzha"+payRs.size());
			while(it.hasNext()){
				String key = it.next();
				//Double v = payRs.get(key);
				Double v = payRs.remove(key);
				if(RaceConfig.LogFlag){
					OperateFile.writeContent(rsLog, key+":"+v);
				}
				//System.out.println("c:"+key+" "+v);
				RaceUtils.updateDataTotair(RaceConfig.TairNamespace, key, v);
			}
		}
	}

}
