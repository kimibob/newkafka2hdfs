package com.newland.newkafka2hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.newland.newkafka2hdfs.utils.HdfsUtil;


public class Main {
	private static final Logger LOG = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

    	if (args.length != 1) {
    		System.out.println("ERROR: Wrong number of parameters instead of 1. check configfile path");
			return;
		}
    	String configPath = args[0];
		Properties prop = new Properties();
		try {
			prop.load(new BufferedInputStream(new FileInputStream(System.getProperty("user.dir") + "/../conf/common_cfg.properties")));
			prop.load(new BufferedInputStream(new FileInputStream(configPath)));
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}
		String dataType = prop.getProperty("dataType");
		System.setProperty("datatype", dataType);
		PropertyConfigurator.configure(System.getProperty("user.dir") + "/../conf/log4j.properties"); 

		Set<Entry<Object, Object>> entrys = prop.entrySet();
        for(Entry<Object, Object> entry:entrys){  
        	LOG.info("{} --> {}", entry.getKey(), entry.getValue());  
        }  
		String roundUnit = prop.getProperty("roundUnit");
		if(!("minute".equals(roundUnit) || "hour".equals(roundUnit) || "day".equals(roundUnit))){
			LOG.error("roundUnit should be [minute | hour | day]!");
			return;
		}

		String exitFile = prop.getProperty("exitFile");
		
		String file = exitFile+dataType;
		File f = new File(file);
		if(f.exists()){
			System.out.println("--->>>Please remove ExitFile: "+file+" before application start!");
			LOG.error("--->>>Please remove ExitFile:{} before application start!",file);
			return;
		}
		
		Configuration conf = new Configuration();
		conf.addResource(Thread.currentThread().getContextClassLoader()
				.getResource("resources/core-site.xml"));
		conf.addResource(Thread.currentThread().getContextClassLoader()
				.getResource("resources/hdfs-site.xml"));
		
		HdfsUtil.initHDFS(conf);
		createConsumerConfig(prop);
		
        ConsumerGroup consumerGroup = new ConsumerGroup(prop);
        consumerGroup.execute();
        
        boolean runFlag = true;
		while(runFlag){
			try {
				Thread.sleep(5000);
				if(f.exists()){
					consumerGroup.shutdown();
					HdfsUtil.getInstance().close();
					runFlag = false;
				}
			} catch (Exception e) {
				e.printStackTrace();
				LOG.error("--->>>Error Occur:{}",e);
			} 
		}
		LOG.info("shutdown at {}...", new Date().toString());
    }

	private static Properties createConsumerConfig(Properties props) {
		//必须配置包含在配置文件中，这个方法用来增加其他可选配置
//		props.put("bootstrap.servers", brokerList);
//		props.put("group.id", groupId);
		props.put("auto.offset.reset", "latest");
		props.put("enable.auto.commit", "false"); 
//		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "60000");
		props.put("key.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.StringDeserializer");
		return props;
	}
}
