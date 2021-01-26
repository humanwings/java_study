package com.humanwings.kafka_study.producer;

import java.util.Properties;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import com.humanwings.kafka_study.common.KafkaConstants;
import com.humanwings.kafka_study.common.MessageInfo;

public class KafkaProducerClientSample {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		
		Properties properties = new Properties();

		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
		
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
		
		KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
		
		sendSynchronous(producer);
		
		sendAsynchronous(producer);
		
		producer.close();
		
	}
	
	/**
	   *  同步发送
	 * @param producer
	 */
	private static void sendSynchronous(KafkaProducer<String, String> producer) {
		
		System.out.println("------------ Synchronous Send----------------");
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(KafkaConstants.TOPIC_NAME, "kafka-demo", "hello. kafka " + System.currentTimeMillis());
		
		try {
			Future<RecordMetadata> dataFuture = producer.send(record);
			
			RecordMetadata daMetadata = dataFuture.get();
			
			System.out.println("topic :" + daMetadata.topic());
			System.out.println("partition :" + daMetadata.partition());
			System.out.println("offset :" + daMetadata.offset());
			
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}
	
	/**
	   * 异步发送
	 * @param producer
	 */
	private static void sendAsynchronous(KafkaProducer<String, String> producer) {
		
		
		System.out.println("------------ Asynchronous Send ----------------");
		
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(KafkaConstants.TOPIC_NAME, "kafka-demo", "hello. kafka " + System.currentTimeMillis());
		
		try {
			
			producer.send(record, new Callback() {
				
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {

					if (exception == null) {
						System.out.println("topic :" + metadata.topic());
						System.out.println("partition :" + metadata.partition());
						System.out.println("offset :" + metadata.offset());
					}
					
				}
			});
			
		}catch(Exception e) {
			e.printStackTrace();
		}
		
	}

}
