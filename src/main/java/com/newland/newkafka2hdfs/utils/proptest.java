package com.newland.newkafka2hdfs.utils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.util.Date;
import java.util.Properties;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class proptest {
	private static final Logger LOG = LoggerFactory.getLogger(proptest.class);

	public static void main(String[] args) {
		System.out.println(System.getProperty("user.dir")+"\\config\\ss.properties");
		System.setProperty("datatype", "gn");
		PropertyConfigurator.configure("D:\\ToolKit\\eclipse-java-luna-SR2-win32-x86_64\\eclipse\\workspace\\newkafka2hdfs\\config\\log4j.properties"); 

		Properties prop = new Properties();
		try {
			prop.load(new BufferedInputStream(new FileInputStream("d:\\b.txt")));
			prop.load(new BufferedInputStream(new FileInputStream("d:\\a.txt")));
		} catch (Exception e) {
			e.printStackTrace();
		}
		String a = prop.getProperty("a");
		String b = prop.getProperty("b");
		LOG.info("{}--->>>{}--->>>{}",new Date(),a,b);
	}
}
	