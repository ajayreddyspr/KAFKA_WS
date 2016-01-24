package com.kafka.consumer;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class IMUKafkaConsumer {

	private final ConsumerConnector consumer;
	private final String topic;

	public IMUKafkaConsumer(String zookeeper, String groupId, String topic) {
		Properties props = new Properties();
		props.put("zookeeper.connect", zookeeper);
		props.put("group.id", groupId);
		props.put("zookeeper.session.timeout.ms", "500");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.commit.enable", "false");
		props.put("queued.max.message.chunks", "10000");

		consumer = Consumer.createJavaConsumerConnector(new ConsumerConfig(
				props));
		this.topic = topic;
	}

	public void topicMessageConsumer() {
		try {
			Map<String, Integer> topicCount = new HashMap();
			topicCount.put(topic, 1);
			Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreams = consumer
					.createMessageStreams(topicCount);
			List<KafkaStream<byte[], byte[]>> streams = consumerStreams
					.get(topic);
			for (final KafkaStream stream : streams) {
				ConsumerIterator<byte[], byte[]> it = stream.iterator();
				if (it.isEmpty())
					System.out.println("Topic is empty-----" + it.size());
				else
					System.out.println("Topic size is-----" + it.size());
				while (it.hasNext()) {
					System.out.println("Message from Single Topic: "
							+ new String(it.next().message()));
				}
			}
			if (consumer != null) {
				consumer.shutdown();
			}
		} catch (Exception ex) {
			System.out.print("error" + ex.getMessage());
		}
	}

	public static void main(String[] args) {
		String topic = "IQUBALTOPIC";
		IMUKafkaConsumer imuKafkaConsumer = new IMUKafkaConsumer(
				"localhost:2181", "kafka-consumer-group", topic);
		imuKafkaConsumer.topicMessageConsumer();
	}

}