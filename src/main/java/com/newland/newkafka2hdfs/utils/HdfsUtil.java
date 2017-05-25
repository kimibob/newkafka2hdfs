package com.newland.newkafka2hdfs.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HdfsUtil {
	private static final Logger LOG = LoggerFactory.getLogger(HdfsUtil.class);

	private static FileSystem hdfs;

	private HdfsUtil() {
	}
	
	public static void initHDFS(Configuration conf){
		if(hdfs == null){
			try {
				hdfs = FileSystem.get(conf); 
				LOG.info("hdfs instance init success~");
			} catch (Exception e) {
				LOG.error("--->>>hdfs init error:{}",e);
			}
        	
        } 
	}

	public static FileSystem getInstance() {  
        return hdfs;  
    }
}
