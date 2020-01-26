package com.prabha.etl;

import org.springframework.stereotype.Component;

@Component
public class JmsMessageHandler {
 public void process(final String message) {
	 System.out.println("-----message-----"+message);
 }
}
