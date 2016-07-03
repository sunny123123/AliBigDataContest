package com.alibaba.middleware.race.model;

import java.io.Serializable;

public class OrderStream implements Serializable{
	private long orderId;//order id
	private String type;//order come from tmall or taobao;
	
	public OrderStream(){}
	
	public OrderStream(long orderId, String type) {
		this.orderId = orderId;
		this.type = type;
	}

	public long getOrderId() {
		return orderId;
	}

	public void setOrderId(long orderId) {
		this.orderId = orderId;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	@Override
	public String toString() {
		return "OrderStream [orderId=" + orderId + ", type=" + type + "]";
	}
	

}
