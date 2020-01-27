package com.prabha.dsl.jms;

import org.springframework.stereotype.Component;

@Component
public class JmsMessageHandler {
 public void process(final String message) {
	 System.out.println("-----JmsMessageHandler.message-----"+message);
 }
}
