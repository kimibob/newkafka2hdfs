package com.newland.newkafka2hdfs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream;
import org.apache.hadoop.hdfs.client.HdfsDataOutputStream.SyncFlag;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.newland.newkafka2hdfs.utils.ConcurrentDateUtil;
import com.newland.newkafka2hdfs.utils.HdfsUtil;

public class ConsumerRunnable implements Runnable {

	private static final Logger LOG = LoggerFactory
			.getLogger(ConsumerRunnable.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);
	// 每个线程维护私有的KafkaConsumer实例
	private final KafkaConsumer<String, String> consumer;
	private String hdfsPath;
	private String threadId;
	private String dataType;
	private int bufferNum;
	private String roundUnit;
	private String topic;
	private HashMap<String, HdfsDataOutputStream> hdfs_os = new HashMap<String, HdfsDataOutputStream>();

	public ConsumerRunnable(Properties props, String threadId) {
		this.topic = props.getProperty("topic");
		this.hdfsPath = props.getProperty("hdfsPath");
		this.dataType = props.getProperty("dataType");
		this.roundUnit = props.getProperty("roundUnit");
		this.bufferNum = Integer.parseInt(props.getProperty("bufferNum"));
		this.consumer = new KafkaConsumer<>(props);
		this.threadId = threadId;
		consumer.subscribe(Arrays.asList(topic));
	}

	@Override
	public void run() {
		try {
			Thread.currentThread().setName(
					this.topic + "-consumerthread-" + threadId);
			LOG.info("consumer: {} start~", Thread.currentThread().getName());
			int buffernum = 0;
			StringBuffer sb = new StringBuffer(5 * 1024 * 1024);
			while (!closed.get()) {
				ConsumerRecords<String, String> records = consumer.poll(1000);
				for (ConsumerRecord<String, String> record : records) {
					// System.out
					// .printf("["
					// + Thread.currentThread().getName()
					// + "] partition = %s offset = %d, key = %s, value = %s%n",
					// record.partition(), record.offset(),
					// record.key(), record.value());
					sb.append(record.value()).append("\r\n");
					buffernum++;
					if (buffernum > this.bufferNum) {

						String filetime = ConcurrentDateUtil.format(new Date());
						String filename = this.hdfsPath
								+ getFileTimePath(filetime, roundUnit)
								+ dataType + "_" + this.threadId;
						data2HDFS(sb.toString(), filename);
						sb.setLength(0);
						buffernum = 0;
						consumer.commitSync();

						// Get the diff of current position and latest offset
						Set<TopicPartition> partitions = new HashSet<TopicPartition>();
						TopicPartition actualTopicPartition = new TopicPartition(
								record.topic(), record.partition());
						partitions.add(actualTopicPartition);
						long actualEndOffset = consumer.endOffsets(partitions)
								.get(actualTopicPartition);
						long actualPosition = consumer
								.position(actualTopicPartition);
						LOG.info(
								"part: {}, diff: {} (endOffset:{}; currentOffset={})",
								actualTopicPartition.partition(),
								actualEndOffset - actualPosition,
								actualEndOffset, actualPosition);
					}
				}
			}
		} catch (WakeupException e) {
			System.err.println(Thread.currentThread().getName()
					+ " WakeupException~~");
			// Ignore exception if closing
			if (!closed.get())
				throw e;
		} finally {
			consumer.commitSync();
			consumer.close();
			Iterator<String> it = hdfs_os.keySet().iterator();
			while (it.hasNext()) {
				String latestFile = it.next();
				try {
					hdfs_os.get(latestFile).close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			System.out.println("consumer stop~");
			LOG.info("consumer: {} stop~");
		}
	}

	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}

	private void data2HDFS(String data, String insertFileTime) {
		FileSystem hdfs = null;
		HdfsDataOutputStream os = null;
		InputStream is = null;
		hdfs = HdfsUtil.getInstance();
		Path hdfsFilePath = null;
		try {
			Iterator<String> it = hdfs_os.keySet().iterator();
			while (it.hasNext()) {
				String latestFile = it.next();
				if (insertFileTime.compareTo(latestFile) > 0) {
					hdfs_os.get(latestFile).close();
					it.remove();
					LOG.info("current file: {} remove last file: {}",
							insertFileTime, latestFile);
				}
			}
			is = new ByteArrayInputStream(data.getBytes());
			hdfsFilePath = new Path(insertFileTime);
			if (!hdfs_os.containsKey(insertFileTime)) {
				if (!hdfs.exists(hdfsFilePath)) {
					os = (HdfsDataOutputStream) hdfs.create(hdfsFilePath);
					os.close();
					os = (HdfsDataOutputStream) hdfs.append(hdfsFilePath);
					hdfs_os.put(insertFileTime, os);
				} else {
					os = (HdfsDataOutputStream) hdfs.append(hdfsFilePath);
					hdfs_os.put(insertFileTime, os);
				}
			} else {
				os = hdfs_os.get(insertFileTime);
				// os = hdfs.append(hdfsFilePath);
			}
			IOUtils.copyBytes(is, os, 5 * 1024 * 1024, false);
			os.hflush();
			os.hsync(EnumSet.of(SyncFlag.UPDATE_LENGTH));
			LOG.debug(" put file: {} to hdfs success!", insertFileTime);
		} catch (RemoteException e) {
			if (AlreadyBeingCreatedException.class.getName().equals(
					e.getClassName())) {
				LOG.info("try to recover fileLease: {}", hdfsFilePath);
				DistributedFileSystem dfs = (DistributedFileSystem) hdfs;
				try {
					dfs.recoverLease(hdfsFilePath);
					boolean isclosed = dfs.isFileClosed(hdfsFilePath);
					Stopwatch sw = new Stopwatch().start();
					while (!isclosed) {
						// soft limit = 60s, hard limit = 3600s
						if (sw.elapsedMillis() > 65 * 1000) {
							e.printStackTrace();
							LOG.error(
									"recover fileLease failed! --->>>data2HDFS error:{}", e);
							throw e;
						}
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e1) {
							e1.printStackTrace();
						}
						isclosed = dfs.isFileClosed(hdfsFilePath);
						LOG.info("recover fileLease: {} => {}!",
								hdfsFilePath, isclosed);
					}
					sw.stop();
					data2HDFS(data, insertFileTime);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
			}else{
				e.printStackTrace();
				LOG.error("--->>>data2HDFS error:{}", e);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOG.error("--->>>data2HDFS error:{}", e);
		} finally {
			IOUtils.closeStream(is);
			// IOUtils.closeStream(os);
		}
	}

	/*
	 * day: 20170518 hour: 20170518/16 minute: 201705181630 or 20170518/1700
	 */
	private String getFileTimePath(String nowtime, String roundUnit) {
		StringBuilder builder = new StringBuilder();
		if ("minute".equals(roundUnit)) {
			builder.append(nowtime.substring(0, 8)).append("/")
					.append(nowtime.substring(8, 10));
			int mm = Integer.parseInt(nowtime.substring(10, 12));
			if (mm >= 0 && mm < 30) {
				builder.append("00");
			} else {
				builder.append("30");
			}
			builder.append("/");
		} else if ("hour".equals(roundUnit)) {
			builder.append(nowtime.substring(0, 8)).append("/")
					.append(nowtime.substring(8, 10)).append("/");
		} else if ("day".equals(roundUnit)) {
			builder.append(nowtime.substring(0, 8)).append("/");
		}
		return builder.toString();
	}

}