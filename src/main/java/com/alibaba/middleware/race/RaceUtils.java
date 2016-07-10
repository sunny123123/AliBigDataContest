package com.alibaba.middleware.race;

import com.alibaba.middleware.race.Tair.TairManageFactory;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.sunny.utils.OperateFileOld;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;


public class RaceUtils {
    /**
     * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
     * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
     * @param object
     * @return
     */
	static DefaultTairManager tairManager = TairManageFactory.getDefaultTairManager();
	private static  Object obj = new Object();
    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input, tClass);
        return ret;
    }
    //make 13 bit time stamp to 10 bit time stamp
    public static long getTimeStamp(long timeStamp){
		 Long minuteTime = (timeStamp / 1000 / 60) * 60;
		return minuteTime;
    }
    public  static void updateDataTotair(int namespace,String key,double value){
		synchronized (obj) {
				
		Result<DataEntry> rs = tairManager.get(namespace, key);
		
		/*put value not exist,put into tair directly*/
		if(rs.getRc().equals(ResultCode.DATANOTEXSITS)){
			//System.out.println("data not exit");
			ResultCode rscode= tairManager.put(namespace, key, value);
			//LOG.info("panzha:putPayTair "+key+" "+value+" not exist,put to tair");
			//OperateFileOld.writeToFile("insert "+key+" "+value +","+"not exist in tair,put into tair,put status:"+rscode.toString());
		}
		else{
			int version = rs.getValue().getVersion();
			double val = (double)rs.getValue().getValue();
			//OperateFileOld.writeToFile(key+" "+value+" exist in tair,get from tair,get status:"+rs.getRc()+",version "+version);
			ResultCode rscode = tairManager.put(namespace, key, value+val,version);
			if(!ResultCode.SUCCESS.equals(rscode)){
				//System.out.println(key+" put failed");
				//LOG.info("panzha:putPayTair "+key+" "+value+" put failed in tair at first time");
				//OperateFileOld.writeToFile(key+" "+value+"current value:"+(value+val)+" put failed in tair at first time");
			}else{
				//System.out.println(key+" put suceess");
				//LOG.info("panzha:putPayTair "+key+" "+value+" put seccess in tair at first time");
				//OperateFileOld.writeToFile("insert "+key+" "+value+"current value:"+(value+val)+" put success at first time,status:"+rscode.toString());
				return ;
			}
			/*version not match ,try to insert,only when version match*/
			while(!ResultCode.SUCCESS.equals(rscode)){
				version = rs.getValue().getVersion();
				val = (double)rs.getValue().getValue();
				rscode = tairManager.put(namespace, key, value+val,version);
				
			}
			//LOG.info("panzha:putPayTair "+key+" "+value+" update suceess in tair"+rscode.toString());
		}
		}
	}
  /*  public void addTradAmountToTair(PaymentStream payStream){
		String rs = tairOP.getOrderRs(RaceConfig.TairNamespace, payStream.getOrderId());
		if(!rs.equals("-1")){
			if(rs.equals("TMALL")){
				tairOP.updateDataTotair(RaceConfig.TairNamespace, RaceConfig.prex_tmall+payStream.getCreateTime(),
						payStream.getPayAmount());
			}
			if(rs.equals("TAOBAO")){
				tairOP.updateDataTotair(RaceConfig.TairNamespace, RaceConfig.prex_taobao+payStream.getCreateTime(),
						payStream.getPayAmount());
			}
		}
		else{
			LOG.info("panzha:getOrderTair failed,add to queue");
			try {
				paymentQueue.put(payStream);//this payment message cannot find order in tair,and add this payment to queue
				
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}*/
}
