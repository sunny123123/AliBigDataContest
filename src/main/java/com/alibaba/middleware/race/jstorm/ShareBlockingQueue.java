package com.alibaba.middleware.race.jstorm;

import java.util.concurrent.LinkedBlockingQueue;

import com.alibaba.middleware.race.model.OrderStream;
import com.alibaba.middleware.race.model.PaymentStream;

public class ShareBlockingQueue {
	
	private static final LinkedBlockingQueue<OrderStream> orderQueue = new LinkedBlockingQueue<OrderStream>();
	private static final LinkedBlockingQueue<PaymentStream> paymentQueue = new LinkedBlockingQueue<PaymentStream>();
	private static final LinkedBlockingQueue<PaymentStream> boltPaymentQueue = new LinkedBlockingQueue<PaymentStream>();
	private ShareBlockingQueue(){}
	
	public static LinkedBlockingQueue<OrderStream> getOrderBlockingQueue(){
		return orderQueue;
	}
	public static LinkedBlockingQueue<PaymentStream> getPaymentBlockingQueue(){
		return paymentQueue;
	}
	public static LinkedBlockingQueue<PaymentStream> getBoltPaymentBlockingQueue(){
		return boltPaymentQueue;
	}
	
}
