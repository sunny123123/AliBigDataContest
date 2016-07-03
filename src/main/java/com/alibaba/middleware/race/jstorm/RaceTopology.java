package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.middleware.race.jstorm.RaceBolt;
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

		builder.setSpout("MetaSpout", new ReadSpout(),1);

		builder.setBolt("MetaBolt", new RaceBolt(),1).shuffleGrouping("MetaSpout");

		return builder;
	}

	private static void submitTopology(TopologyBuilder builder,String submitMode) {
		Config conf = new Config();
		conf.setNumWorkers(1);
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
