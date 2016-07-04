package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_34871wea6u_";
    public static String prex_taobao = "platformTaobao_34871wea6u_";
    public static String prex_ratio = "ratio_34871wea6u_";


    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static final String MQNameServerAddr = "10.124.22.213:9876";
    public static final String JstormTopologyName = "34871wea6u";
    public static final String MetaConsumerGroup = "34871wea6u";
    public static final String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static final String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static final String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    
    public static final String TairConfigServer = "10.124.10.11:5198";  //local config
    //public static final String TairConfigServer = "10.101.72.127:5198"; //alibaba config
    //public static final String TairSalveConfigServer = "10.101.72.128:5198";
    public static final String TairGroup = "group_1";    //local config
    //public static final String TairGroup = "group_tianchi"; //alibaba config
    public static final Integer TairNamespace = 1;
    //public static final Integer TairNamespace = 57917;
}
