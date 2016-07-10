package com.alibaba.middleware.race.jstorm;

import java.io.BufferedWriter;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
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
import com.sunny.backup.PaymentMessage;
import com.sunny.utils.CommonShareData;
import com.sunny.utils.OperateFile;
import com.sunny.utils.OperateFileOld;

public class RaceSendSpout implements IRichSpout,MessageListenerConcurrently{
	
	Logger LOG = LoggerFactory.getLogger(RaceSendSpout.class); 
	
	SpoutOutputCollector collector;
	AtomicInteger tmallOrderNum = null;
	AtomicInteger taobaoOrderNum = null;
	AtomicInteger payOrderNum = null;
	AtomicInteger OrderNum = null;
	LinkedBlockingQueue<OrderStream> orderQueue = null;
	LinkedBlockingQueue<PaymentStream> paymentQueue = null;
	BufferedWriter orderSendLog = null;
	DefaultMQPushConsumer consumer = null;
	BufferedWriter paySendLog = null;
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream("order", new Fields("order"));
		declarer.declareStream("payment", new Fields("payment"));
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
		if(RaceConfig.LogFlag){
			this.orderSendLog = OperateFile.getWriter(context.getThisComponentId().toLowerCase()+"_"+context.getThisTaskId()+
					"_"+"sendorder");
			this.paySendLog = OperateFile.getWriter(context.getThisComponentId().toLowerCase()+"_"+context.getThisTaskId()+
					"_"+"sendpay");
		}
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
		if(RaceConfig.LogFlag){
			OperateFile.writeContent(orderSendLog, "send total order entries:"+this.OrderNum);
			OperateFile.writeContent(paySendLog, "send total payment entries:"+this.payOrderNum);
		}
		//OperateFileOld.writeToFile("taoorderNUM:"+taobaoOrderNum+",tmallorderNUM:"+tmallOrderNum);
	}

	public void nextTuple() {
		
		OrderStream order;
		try {
			if(orderQueue.size()!=0){
				order = orderQueue.take();
				collector.emit("order",new Values(order));
				
				if(RaceConfig.LogFlag)
					OperateFile.writeContent(orderSendLog, "order spout send:"+order.toString());
				//LOG.info("panzha:send to bolt: "+order.toString());
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		PaymentStream pay;
		try {
			if(paymentQueue.size()!=0){
				pay = paymentQueue.take();
				collector.emit("payment",new Values(pay));
				
				if(RaceConfig.LogFlag)
					OperateFile.writeContent(paySendLog, "payment spout send:"+pay.toString());
				//LOG.info("panzha:send to bolt: "+pay.toString());
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void ack(Object msgId) {
		//OperateFile.writeToFile(msgId+" emit ack");
	}

	public void fail(Object msgId) {
		//OperateFile.writeToFile(msgId+" emit failed");
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
	            //LOG.info("panzha:tmallorder received from mq"+msg.getKeys()+" "+msg.getMsgId()+" "+tmallOrder.toString());
	            //System.out.println(tmallOrder);
				break;
			case RaceConfig.MqTaobaoTradeTopic:
				if (body.length == 2 && body[0] == 0 && body[1] == 0){
	                //System.out.println(RaceConfig.MqTaobaoTradeTopic+" Got the end signal");
					//LOG.info(RaceConfig.MqTaobaoTradeTopic+" Got the end signal");
	                continue;
	            }
				 OrderMessage taobaoOrder = RaceUtils.readKryoObject(OrderMessage.class, body);
				 try {
					orderQueue.put(new OrderStream(taobaoOrder.getOrderId(), "TAOBAO"));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				 //System.out.println(taobaoOrder);
				 taobaoOrderNum.getAndIncrement();
				 OrderNum.getAndIncrement();
				 //LOG.info("TAOBAOORDER:"+String.valueOf(taobaoOrderNum.getAndIncrement()+1));
				 //LOG.info("panzha:taobaoorder received from mq"+msg.getKeys()+" "+msg.getMsgId()+" "+taobaoOrder.toString());
				break;
			case RaceConfig.MqPayTopic:
				if (body.length == 2 && body[0] == 0 && body[1] == 0){
	                //System.out.println(RaceConfig.MqPayTopic+" Got the end signal");
	                LOG.info(RaceConfig.MqPayTopic+" Got the end signal");
	                
	         /*       try {
						paymentQueue.put(new PaymentStream(-1111L, 0.0, (short)0,0L));
					} catch (InterruptedException e) {
						e.printStackTrace();
					}*/
	                continue;
	            }
				 PaymentMessage pay = RaceUtils.readKryoObject(PaymentMessage.class, body);
				 try {
					paymentQueue.put(new PaymentStream(pay.getOrderId(), pay.getPayAmount(), pay.getPayPlatform(), 
							 RaceUtils.getTimeStamp(pay.getCreateTime())));
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				 //System.out.println(pay);
				 payOrderNum.getAndIncrement();
				 //LOG.info("PAYMENT:"+String.valueOf(payOrderNum.getAndIncrement()+1));
				 //LOG.info("panzha:payment received from mq"+msg.getKeys()+" "+msg.getMsgId()+" "+pay.toString());
				break;
			default:
				break;
			}
            
		}
		return 	 ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
	}
		
	public enum SubscribleTopic{
		 MiddlewareRaceTestData_Pay,
		 MiddlewareRaceTestData_TMOrder,
		 MiddlewareRaceTestData_TBOrder,
	}

}
