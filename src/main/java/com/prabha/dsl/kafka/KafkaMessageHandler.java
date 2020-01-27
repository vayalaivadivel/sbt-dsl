package com.prabha.dsl.kafka;

import org.springframework.stereotype.Component;

@Component
public class KafkaMessageHandler {
 public void process(final String message) {
	 System.out.println("-----KafkaMessageHandler.message-----"+message);
 }
}
