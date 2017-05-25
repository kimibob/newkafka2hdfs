package com.newland.newkafka2hdfs;


import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ConsumerGroup {

    private List<ConsumerRunnable> consumers;
    private ExecutorService executor;
    private int consumerNum;
    private Properties prop;
    
    public ConsumerGroup(Properties prop) {
    	this.prop = prop;
    	consumerNum = Integer.parseInt(prop.getProperty("threadNum"));
    	executor = Executors.newFixedThreadPool(consumerNum);
        consumers = new ArrayList<>(consumerNum);

    }

    public void execute() {
        for (int i = 0; i < consumerNum; i++) {
            ConsumerRunnable consumerThread = new ConsumerRunnable(this.prop, i+"");
            consumers.add(consumerThread);
            executor.submit(consumerThread);
        }
    }
    
	public void shutdown() throws InterruptedException {
		for (ConsumerRunnable consumerRunnable : consumers) {
			consumerRunnable.shutdown();
		}
		executor.shutdown();
		executor.awaitTermination(5, TimeUnit.SECONDS);
	}
}