package com.sunny.backup;

import java.io.BufferedWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.alibaba.middleware.race.ConsumerPushFactory;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.OrderStream;
import com.alibaba.middleware.race.model.PaymentStream;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.sunny.utils.CommonShareData;
import com.sunny.utils.OperateFile;
import com.sunny.utils.OperateFileOld;

public class SendPaymentSpout implements IRichSpout,MessageListenerConcurrently{
	
	Logger LOG = LoggerFactory.getLogger(SendPaymentSpout.class); 
	
	SpoutOutputCollector collector;
	AtomicInteger tmallOrderNum = null;
	AtomicInteger taobaoOrderNum = null;
	AtomicInteger payOrderNum = null;
	AtomicInteger OrderNum = null;
	LinkedBlockingQueue<OrderStream> orderQueue = null;
	LinkedBlockingQueue<PaymentStream> paymentQueue = null;
	DefaultMQPushConsumer consumer = null;
	BufferedWriter paySendLog = null;
	BufferedWriter orderSendLog = null;
	ThreadPoolExecutor executor = null;
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("orderId","Payment"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		consumer = ConsumerPushFactory.getConsumer(this);
		orderQueue = new LinkedBlockingQueue<OrderStream>();
		paymentQueue = new LinkedBlockingQueue<PaymentStream>();
		LOG.info("panzha:consumer start reading data");
		this.OrderNum = new AtomicInteger(0);
		this.taobaoOrderNum = new AtomicInteger(0);
		this.tmallOrderNum = new AtomicInteger(0);
		this.payOrderNum = new AtomicInteger(0);
		
		this.paySendLog = OperateFile.getWriter(context.getThisComponentId().toLowerCase()+"_"+context.getThisTaskId());
		this.orderSendLog = OperateFile.getWriter(context.getThisComponentId().toLowerCase()+"_"+context.getThisTaskId());
		
		executor = new ThreadPoolExecutor(2, 4, 200, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(2));
		
		new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					consumer.start();
				} catch (MQClientException e) {
					e.printStackTrace();
				}
			}
		}).start();
	}

	public void close() {
	}

	public void activate() {
	}

	public void deactivate() {
		//OperateFileOld.writeToFile("payNUM:"+payOrderNum);
		OperateFile.writeContent(paySendLog, "send total order entries:"+this.payOrderNum);
	}

	public void nextTuple() {
		
	}

	public void ack(Object msgId) {
		//OperateFile.writeToFile(msgId+" emit ack");
	}

	public void fail(Object msgId) {
		//OperateFile.writeToFile(msgId+"emit failed");
	}

	public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
			ConsumeConcurrentlyContext context) {
		
		for (MessageExt msg : msgs) {
			byte [] body = msg.getBody();
			String topic = context.getMessageQueue().getTopic();
		
			switch (topic) {
			case RaceConfig.MqTmallTradeTopic:
				
	            if (body.length == 2 && body[0] == 0 && body[1] == 0){
	                //System.out.println(RaceConfig.MqTmallTradeTopic+" Got the end signal");
	            	LOG.info(RaceConfig.MqTmallTradeTopic+" Got the end signal");
	                continue;
	            }
	            OrderMessage tmallOrder = RaceUtils.readKryoObject(OrderMessage.class, body);
	            try {
					orderQueue.put(new OrderStream(tmallOrder.getOrderId(), "TMALL"));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
	            tmallOrderNum.getAndIncrement();
	            OrderNum.getAndIncrement();
	            //LOG.info("TMALLORDER:"+String.valueOf(tmallOrderNum.getAndIncrement()+1));
	            LOG.info("panzha:tmallorder received from mq"+msg.getKeys()+" "+msg.getMsgId()+" "+tmallOrder.toString());
	            //System.out.println(tmallOrder);
				break;
			case RaceConfig.MqTaobaoTradeTopic:
				if (body.length == 2 && body[0] == 0 && body[1] == 0){
	                //System.out.println(RaceConfig.MqTaobaoTradeTopic+" Got the end signal");
					LOG.info(RaceConfig.MqTaobaoTradeTopic+" Got the end signal");
	                continue;
	            }
				 OrderMessage taobaoOrder = RaceUtils.readKryoObject(OrderMessage.class, body);
				 try {
					orderQueue.put(new OrderStream(taobaoOrder.getOrderId(), "TAOBAO"));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				 taobaoOrderNum.getAndIncrement();
				 OrderNum.getAndIncrement();
				 //System.out.println(taobaoOrder);
				 //LOG.info("TAOBAOORDER:"+String.valueOf(taobaoOrderNum.getAndIncrement()+1));
				 LOG.info("panzha:taobaoorder received from mq"+msg.getKeys()+" "+msg.getMsgId()+" "+taobaoOrder.toString());
				break;
			case RaceConfig.MqPayTopic:
				if (body.length == 2 && body[0] == 0 && body[1] == 0){
	                //System.out.println(RaceConfig.MqPayTopic+" Got the end signal");
	                LOG.info(RaceConfig.MqPayTopic+" Got the end signal");
	                continue;
	            }
				 PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class, body);
				 try {
					paymentQueue.put(new PaymentStream(pay.getOrderId(), pay.getPayAmount(), pay.getPayPlatform(), 
							 RaceUtils.getTimeStamp(pay.getCreateTime())));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				 payOrderNum.getAndIncrement();
				 //System.out.println(pay);
				 // LOG.info("PAYMENT:"+String.valueOf(payOrderNum.getAndIncrement()+1));
				 LOG.info("panzha:payment received from mq"+msg.getKeys()+" "+msg.getMsgId()+" "+pay.toString());
				break;
			default:
				break;
			}
            
		}
		return 	 ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	}
	
	class SendOrder implements Runnable{

		@Override
		public void run() {
			while(true){
				OrderStream order;
				try {
					order = orderQueue.take();
					collector.emit(new Values(order.getOrderId(),order));
					OperateFile.writeContent(orderSendLog, "order spout send:"+order.toString());
					//LOG.info("panzha:send to bolt: "+order.toString());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
		
	}
	class SendPayment implements Runnable{

		@Override
		public void run() {
			while(true){
				PaymentStream pay;
				try {
					pay = paymentQueue.take();
					collector.emit(new Values(pay.getOrderId(),pay));
					OperateFile.writeContent(paySendLog, "payment spout send:"+pay.toString());
					//LOG.info("panzha:send to bolt: "+pay.toString());
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			
		}
		
	}

}
