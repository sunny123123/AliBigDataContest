package com.alibaba.middleware.race.model;

import java.io.Serializable;

public class PaymentStream implements Serializable{
	private long orderId;
	private double payAmount;
	private short payPlatform; 
	private long createTime ;
	
	public PaymentStream() {
	}

	public PaymentStream(long orderId, double payAmount, short payPlatform,
			long createTime) {
		this.orderId = orderId;
		this.payAmount = payAmount;
		this.payPlatform = payPlatform;
		this.createTime = createTime;
	}

	public long getOrderId() {
		return orderId;
	}

	public void setOrderId(long orderId) {
		this.orderId = orderId;
	}

	public double getPayAmount() {
		return payAmount;
	}

	public void setPayAmount(double payAmount) {
		this.payAmount = payAmount;
	}

	public short getPayPlatform() {
		return payPlatform;
	}

	public void setPayPlatform(short payPlatform) {
		this.payPlatform = payPlatform;
	}

	public long getCreateTime() {
		return createTime;
	}

	public void setCreateTime(long createTime) {
		this.createTime = createTime;
	}

	@Override
	public String toString() {
		return "PaymentStream [orderId=" + orderId + ", payAmount=" + payAmount
				+ ", payPlatform=" + payPlatform + ", createTime=" + createTime
				+ "]";
	}
}
