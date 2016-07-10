package com.alibaba.middleware.race.model;

import java.io.Serializable;

public class RatioStream implements Serializable{
	private double wireMoney;
	private double pcMoney;
	
	
	public RatioStream() {
	}

	public RatioStream(double wireMoney, double pcMoney) {
		this.wireMoney = wireMoney;
		this.pcMoney = pcMoney;
	}

	public double getWireMoney() {
		return wireMoney;
	}

	public void setWireMoney(double wireMoney) {
		this.wireMoney = wireMoney;
	}

	public double getPcMoney() {
		return pcMoney;
	}

	public void setPcMoney(double pcMoney) {
		this.pcMoney = pcMoney;
	}
	
}
