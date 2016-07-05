package com.sunny.utils;

import com.alibaba.middleware.race.RaceUtils;

import clojure.main;

public class Test {
	public static void main(String[] args) {
		OperateFile.writeToFile("hello world100");
		System.out.println(RaceUtils.getTimeStamp(System.currentTimeMillis()));
	}
	
}
