package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

public class ConsumerPushFactory {
	public static DefaultMQPushConsumer getConsumer(MessageListenerConcurrently listener){
		DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(RaceConfig.MetaConsumerGroup);
		//consumer.setNamesrvAddr(RaceConfig.MQNameServerAddr);
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.registerMessageListener(listener);
		try {
			consumer.subscribe(RaceConfig.MqTaobaoTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqTmallTradeTopic, "*");
			consumer.subscribe(RaceConfig.MqPayTopic, "*");
		} catch (MQClientException e) {
			e.printStackTrace();
		}
		
		return consumer;
	}

}
