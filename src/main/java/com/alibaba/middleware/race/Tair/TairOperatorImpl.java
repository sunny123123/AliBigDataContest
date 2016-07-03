package com.alibaba.middleware.race.Tair;

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;


/**
 * 读写tair所需要的集群信息，如masterConfigServer/slaveConfigServer地址/
 * group 、namespace我们都会在正式提交代码前告知选手
 */
public class TairOperatorImpl {
	Logger LOG = LoggerFactory.getLogger(TairOperatorImpl.class); 
	public TairOperatorImpl(){}
	
    public TairOperatorImpl(String masterConfigServer,
                            String slaveConfigServer,
                            String groupName,
                            int namespace) {
    }

    public boolean write(Serializable key, Serializable value) {
        return false;
    }

    public Object get(Serializable key) {
        return null;
    }

    public boolean remove(Serializable key) {
        return false;
    }

    public void close(){
    }
    /*
     * get data from tair
     */
    public  String getOrderRs(int namespace,long key){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		Result<DataEntry> rs = tairManager.get(namespace, key);
		if(ResultCode.SUCCESS.equals(rs.getRc())){
			return (String)rs.getValue().getValue();
		}
		return "-1";//cannot find key
		
		//System.out.println(rs);
	}
    /*
     * get data from tair
     */
    public  void getPaymentRs(int namespace,String key){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		Result<DataEntry> rs = tairManager.get(namespace, key);
		//System.out.println(rs);
	}
    /*
     * put data to tair
     */
    public  void putPayment(int namespace,String key,double value){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		ResultCode rscode = tairManager.put(namespace, key, value);
		//System.out.println(rscode);
	}
    
    /*
     * put data to tair
     */
    public  void putOrder(int namespace,long key,String value){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		ResultCode rscode = tairManager.put(namespace, key, value);
		LOG.info("panzha:putOrderToTair "+key+" "+value);
		//System.out.println(rscode);
	}
    /*
     * if data not exist,and write directly
     * if data exist,and update
     */
    public  void updateDataTotair(int namespace,String key,double value){
		DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
		Result<DataEntry> rs = tairManager.get(1, key);
		if(rs.getRc().equals(ResultCode.DATANOTEXSITS)){
			//System.out.println("data not exit");
			tairManager.put(namespace, key, value);
			LOG.info("panzha:putPayTair "+key+" "+value+" not exist,put to tair");
		}
		else{
			if(ResultCode.SUCCESS.equals(rs.getRc())){
				//System.out.println("data get success");
				LOG.info("panzha:putPayTair "+key+" "+value+" find in tair");
			}
			int version = rs.getValue().getVersion();
			double val = (double)rs.getValue().getValue();
			ResultCode rscode = tairManager.put(namespace, key, value+val,version);
			if(!ResultCode.SUCCESS.equals(rscode)){
				//System.out.println(key+" put failed");
				LOG.info("panzha:putPayTair "+key+" "+value+" put failed in tair at first time");
			}else{
				//System.out.println(key+" put suceess");
				LOG.info("panzha:putPayTair "+key+" "+value+" put seccess in tair at first time");
			}
			
			while(!ResultCode.SUCCESS.equals(rscode)){
				version = rs.getValue().getVersion();
				val = (double)rs.getValue().getValue();
				rscode = tairManager.put(namespace, key, value+val,version);
			}
			LOG.info("panzha:putPayTair "+key+" "+value+" update suceess in tair");
		}
	}
    //天猫的分钟交易额写入tair
    public static void main(String [] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //假设这是付款时间
        Long millisTime = System.currentTimeMillis();
        //由于整分时间戳是10位数，所以需要转换成整分时间戳
        Long minuteTime = (millisTime / 1000 / 60) * 60;
        //假设这一分钟的交易额是100;
        Double money = 100.0;
        //写入tair
        tairOperator.write(RaceConfig.prex_tmall + minuteTime, money);
    }
}
