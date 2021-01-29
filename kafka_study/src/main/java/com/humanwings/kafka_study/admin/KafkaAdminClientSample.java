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

import com.humanwings.kafka_study.common.KafkaConstants;

public class KafkaAdminClientSample {
	
	private final static String TEMP_TOPIC_NAME = "Topic-4-4";
	
	public static void main(String[] args) throws Exception{
		// 

		AdminClient client = adminClient();
		
		System.out.println("---------------------------------------------------------------------");
		System.out.println("Admin Client :" + client);
		createTopics(client);
		//listTopics(client);
		//deleteTopic(client);
		//listTopics(client);
	}
	
	private static AdminClient adminClient() {
		
		Properties properties = new Properties();
		
		properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVERS);
		
		return AdminClient.create(properties);
	}
	
	
	/**
	 * Create Topic
	 * @param client
	 */
	private static void createTopics(AdminClient client) throws Exception{
		
		NewTopic topic = new NewTopic(TEMP_TOPIC_NAME, 4, (short)4);
		
		CreateTopicsResult result = client.createTopics(Arrays.asList(topic));
		
		System.out.println("---------------------------------------------------------------------");
		System.out.println("Create topic result : " + result.all().get());
		
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
