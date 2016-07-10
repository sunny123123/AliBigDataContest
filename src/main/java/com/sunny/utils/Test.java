package com.sunny.utils;

import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.RatioStream;

import clojure.main;

public class Test {
	public static void main(String[] args) {
		//OperateFileOld.writeToFile("hello world100");
		//System.out.println(RaceUtils.getTimeStamp(System.currentTimeMillis()));
		RatioStream r = new RatioStream();
		System.out.println(r.getPcMoney());
	}
	
}
