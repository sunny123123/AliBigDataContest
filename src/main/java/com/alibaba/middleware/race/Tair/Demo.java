package com.alibaba.middleware.race.Tair;

import java.awt.image.RescaleOp;
import java.util.ArrayList;
import java.util.List;

import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.comm.TairClient;
import com.taobao.tair.extend.TairManagerList;
import com.taobao.tair.impl.DefaultTairManager;

import clojure.main;

public class Demo {
	public static void main(String[] args) {
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		//test2();
		
		//updateDataTotair(1,"tmall001",100.02);
		
		//put(1,"tmall002",100.02);
		getRs(1,"tmall002");
		//Result<DataEntry> rs = tairManager.get(1, "tmall001");
		//System.out.println(rs);
		//tairManager.
		
		//System.out.println(rs.getValue());
		//tairManager.put(0, "001", "tomcat");
		//tairManager.put(0, "002", "apache");
		//tairManager.put(0, "003", "jboss");
		
		//Result<DataEntry> rs = tairManager.get(0, "002");
		//System.out.println(rs.getValue());
		//tairManager.getItems(namespace, key, offset, count)
		tairManager.close();
		
	}
	public static void put(int namespace,String key,double value){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		ResultCode rscode = tairManager.put(namespace, key, value,10);
		System.out.println(rscode);
	}
	public static void getRs(int namespace,String key){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		Result<DataEntry> rs = tairManager.get(namespace, key);
		System.out.println(rs);
	}
	public static void updateDataTotair(int namespace,String key,double value){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		Result<DataEntry> rs = tairManager.get(1, key);
		if(rs.getRc().equals(ResultCode.DATANOTEXSITS)){
			System.out.println("data not exit");
			tairManager.put(namespace, key, value);
		}
		else{
			if(ResultCode.SUCCESS.equals(rs.getRc())){
				System.out.println("data get success");
			}
			int version = rs.getValue().getVersion();
			double val = (double)rs.getValue().getValue();
			ResultCode rscode = tairManager.put(namespace, key, value+val,version);
			if(!ResultCode.SUCCESS.equals(rscode)){
				System.out.println(key+" put failed");
			}else{
				System.out.println(key+" put suceess");
			}
			
			while(!ResultCode.SUCCESS.equals(rscode)){
				version = rs.getValue().getVersion();
				val = (double)rs.getValue().getValue();
				rscode = tairManager.put(namespace, key, value+val,version);
			}
		}
	}
	public static void test1(){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		
		Result<DataEntry> rs = tairManager.get(1, "name");
		if(rs.getRc().equals(ResultCode.DATANOTEXSITS))
			System.out.println("data not exit");
		else{
			if(ResultCode.SUCCESS.equals(rs.getRc())){
				System.out.println("data get success");
			}
			int v = rs.getValue().getVersion();
			ResultCode rscode = tairManager.put(1, "name", "apache",v);
			while(!ResultCode.SUCCESS.equals(rscode)){
				v = rs.getValue().getVersion();
				rscode = tairManager.put(1, "name", "apache",v);
			}
		}
		System.out.println(rs);
		//ResultCode code = tairManager.put(1, "name", "tomcat20",1);
		//System.out.println(code);
		
		tairManager.close();
	
	}
}
