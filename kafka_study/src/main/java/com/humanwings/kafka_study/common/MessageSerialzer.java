package com.humanwings.kafka_study.common;

import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.kafka.common.serialization.Serializer;
import org.hibernate.validator.cfg.context.ReturnValueConstraintMappingContext;

public class MessageSerialzer implements Serializer<MessageInfo>{
	
	public void configure(Map configs,boolean isKey ) {
		
	}

	public byte[] serialize(String topic,MessageInfo info ) {
		
		if (info == null) {
			return null;
		}
		
		byte[] title;
		
		try {
			if (info.getMessageTitle() != null) {
				title = info.getMessageTitle().getBytes("UTF-8");
			}else {
				title = new byte[0];
			}
			ByteBuffer buffer = ByteBuffer.allocate(4 + 4 + title.length);
			
			buffer.putInt(info.getMessageId());
			buffer.putInt(title.length);
			buffer.put(title);
			return buffer.array();
		}catch (Exception e) {
			e.printStackTrace();
		}
		
		return new byte[0];
		
	}
	
	public void close() {
		
	}
	
}
