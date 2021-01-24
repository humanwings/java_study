package com.humanwings.kafka_study.admin;

import java.util.Arrays;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

public class KafkaAdminClientSample {
	
	private final static String TOPIC_NAME = "humanwings-dev"; 
	
	private final static String TEMP_TOPIC_NAME = "temp-topic";

	public static void main(String[] args) throws Exception{
		// 

		AdminClient client = adminClient();
		
		System.out.println("---------------------------------------------------------------------");
		System.out.println("Admin Client :" + client);
		createTopics(client);
		listTopics(client);
		deleteTopic(client);
		listTopics(client);
	}
	
	private static AdminClient adminClient() {
		
		Properties properties = new Properties();
		
		properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "35.72.34.152:9092");
		
		return AdminClient.create(properties);
	}
	
	
	/**
	 * Create Topic
	 * @param client
	 */
	private static void createTopics(AdminClient client) {
		
		NewTopic topic = new NewTopic(TEMP_TOPIC_NAME, 1, (short)1);
		
		CreateTopicsResult topics = client.createTopics(Arrays.asList(topic));
		
		System.out.println("---------------------------------------------------------------------");
		System.out.println("Create topic result : " + topics);
		
	}
	
	/**
	 * List Topic
	 * @param client
	 */
	private static void listTopics(AdminClient client) throws Exception{
		
		ListTopicsResult listResult = client.listTopics();
		
		Set<String> names = listResult.names().get();
		
		System.out.println("---------------------------------------------------------------------");
		names.stream().forEach(System.out::println);
		
	}
	
	/**
	 * delete Topic
	 * @param client
	 */
	private static void deleteTopic(AdminClient client) throws Exception{
		
		DeleteTopicsResult result = client.deleteTopics(Arrays.asList(TEMP_TOPIC_NAME));
		
		result.all().get();
		
	}
}
