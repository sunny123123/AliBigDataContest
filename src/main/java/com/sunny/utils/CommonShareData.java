package com.sunny.utils;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.model.OrderStream;
import com.alibaba.middleware.race.model.PaymentStream;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class CommonShareData {
	
	private static final LinkedBlockingQueue<OrderStream> orderQueue = new LinkedBlockingQueue<OrderStream>();
	private static final LinkedBlockingQueue<PaymentStream> paymentQueue = new LinkedBlockingQueue<PaymentStream>();
	private static final LinkedBlockingQueue<PaymentStream> boltPaymentQueue = new LinkedBlockingQueue<PaymentStream>();
	
	private static final AtomicInteger tmallOrderNum = new AtomicInteger(0);
	private static final AtomicInteger taobaoOrderNum = new AtomicInteger(0);
	private static final AtomicInteger payOrderNum = new AtomicInteger(0);
	private static final AtomicInteger OrderNum = new AtomicInteger(0);
	
	private static DefaultMQPushConsumer consumer = null;
	
	private CommonShareData(){}
	
	public static LinkedBlockingQueue<OrderStream> getOrderBlockingQueue(){
		return orderQueue;
	}
	public static LinkedBlockingQueue<PaymentStream> getPaymentBlockingQueue(){
		return paymentQueue;
	}
	public static LinkedBlockingQueue<PaymentStream> getBoltPaymentBlockingQueue(){
		return boltPaymentQueue;
	}
	
	public static synchronized DefaultMQPushConsumer getMQPushConsumer(MessageListenerConcurrently listener){
		if(consumer==null){
			consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
			consumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
			
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.registerMessageListener(listener);
			try {
				consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
				consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
				consumer.subscribe(RaceConfig.MqPayTopic, "*");
			} catch (MQClientException e) {
				e.printStackTrace();
			}
		}
		return consumer;
	}

	public static AtomicInteger getTmallOrderNum() {
		return tmallOrderNum;
	}

	public  static AtomicInteger getTaobaoOrderNum() {
		return taobaoOrderNum;
	}

	public static AtomicInteger getPayOrderNum() {
		return payOrderNum;
	}

	public static AtomicInteger getOrderNum() {
		return OrderNum;
	}
	
}
