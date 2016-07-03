package com.alibaba.middleware.race.Tair;

import java.util.ArrayList;
import java.util.List;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.impl.DefaultTairManager;

public class TairManageFactory {
	private static  DefaultTairManager tairManager = null;
	private TairManageFactory(){}
	public static synchronized DefaultTairManager  getDefaultTairManager(){
		
		if(tairManager!=null)
			return tairManager;
		tairManager = new DefaultTairManager();
		List<String> confServer = new ArrayList<String>();
		confServer.add(RaceConfig.TairConfigServer);
		tairManager.setConfigServerList(confServer);
		tairManager.setGroupName(RaceConfig.TairGroup);
		
		tairManager.init();
		return tairManager;
	}
}
