package com.alibaba.middleware.race.jstorm;

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

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.OrderStream;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.model.PaymentStream;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.sunny.utils.OperateFile;

public class SendPaymentSpout implements IRichSpout,MessageListenerConcurrently{
	
	Logger LOG = LoggerFactory.getLogger(SendPaymentSpout.class); 
	
	SpoutOutputCollector collector;
	AtomicInteger tmallOrderNum = new AtomicInteger(0);
	AtomicInteger taobaoOrderNum = new AtomicInteger(0);
	AtomicInteger payOrderNum = new AtomicInteger(0);
	LinkedBlockingQueue<OrderStream> orderQueue = null;
	LinkedBlockingQueue<PaymentStream> paymentQueue = null;
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sendmessage"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		DefaultMQPushConsumer consumer = ConsumerPushFactory.getConsumer(this);
		orderQueue = ShareBlockingQueue.getOrderBlockingQueue();
		paymentQueue = ShareBlockingQueue.getPaymentBlockingQueue();
		LOG.info("panzha:consumer start reading data");
		try {
			consumer.start();
		} catch (MQClientException e) {
			LOG.error("panzha:consumergroup start failed");
			e.printStackTrace();
		}
	}

	public void close() {
		// TODO Auto-generated method stub
		
	}

	public void activate() {
		// TODO Auto-generated method stub
		
	}

	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	public void nextTuple() {
		
		try {
			
			PaymentStream pay = paymentQueue.take();
			this.collector.emit(new Values(pay));
			
			LOG.info("panzha:send to bolt: "+pay.toString());
			
		} catch (InterruptedException e) {
			
			e.printStackTrace();
		}
	}

	public void ack(Object msgId) {
		// TODO Auto-generated method stub
		
	}

	public void fail(Object msgId) {
		// TODO Auto-generated method stub
		OperateFile.writeToFile(msgId+"emit failed");
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
	            LOG.info("TMALLORDER:"+String.valueOf(tmallOrderNum.getAndIncrement()+1));
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
				 //System.out.println(taobaoOrder);
				 LOG.info("TAOBAOORDER:"+String.valueOf(taobaoOrderNum.getAndIncrement()+1));
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
				 //System.out.println(pay);
				  LOG.info("PAYMENT:"+String.valueOf(payOrderNum.getAndIncrement()+1));
				 LOG.info("panzha:payment received from mq"+msg.getKeys()+" "+msg.getMsgId()+" "+pay.toString());
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
