package com.alibaba.middleware.race.jstorm;

import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import com.alibaba.middleware.race.RaceConfig;

public class RaceTopology {
	static Logger LOG = LoggerFactory.getLogger(RaceTopology.class); 
	//private static Map conf = new HashMap<Object, Object>();
	
	public static void main(String[] args) {
		TopologyBuilder builder = null;
		try {
			builder = setupBuilder();
		} catch (Exception e) {
	
			e.printStackTrace();
		}

		//submitTopology(builder,"LOCAL");
		submitTopology(builder,"CLUSTER");

	}
	private static TopologyBuilder setupBuilder() throws Exception {
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("SendSpout", new RaceSendSpout(),RaceConfig.SendSouptParallelism);
		
		builder.setBolt("OrderBolt", new RaceOrderBolt(),RaceConfig.OrderBoltParallelism).shuffleGrouping("SendSpout", "order");
		builder.setBolt("PaymentBolt", new RacePayBolt(),RaceConfig.PayBoltParallelism).shuffleGrouping("SendSpout", "payment");
		

		builder.setBolt("MergeOderPayment", new MergeOrderPayment(),RaceConfig.MerageBoltParallelism).fieldsGrouping("OrderBolt", new Fields("orderId")).
			fieldsGrouping("PaymentBolt", new Fields("orderId"));
		
		//builder.setBolt("MetaBolt2", new RacePayBolt(),2).shuffleGrouping("MetaPaymentSpout");
		
		//builder.setBolt("MetaBolt1", new RaceMergeBolt(),4).fieldsGrouping("MetaOrderSpout", new Fields("orderId"))
		//.fieldsGrouping("MetaPaymentSpout",new Fields("orderId"));
		

		return builder;
	}

	private static void submitTopology(TopologyBuilder builder,String submitMode) {
		Config conf = new Config();

		conf.setNumWorkers(RaceConfig.WorkersNum);
		//conf.setNumAckers(1);

		try {
			if (submitMode=="LOCAL") {

				LocalCluster cluster = new LocalCluster();

				cluster.submitTopology(
						String.valueOf(RaceConfig.JstormTopologyName), conf,
						builder.createTopology());
				//Thread.sleep(200000);
				//cluster.shutdown();
			} else {
				StormSubmitter.submitTopology(
						String.valueOf(RaceConfig.JstormTopologyName), conf,
						builder.createTopology());
			}

		} catch (Exception e) {
			LOG.error(e.getMessage(), e.getCause());
		}
	}
}
