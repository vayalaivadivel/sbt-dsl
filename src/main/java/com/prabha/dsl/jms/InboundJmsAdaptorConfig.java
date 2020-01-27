package com.prabha.dsl.jms;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.jms.dsl.Jms;
import org.springframework.jms.core.JmsTemplate;

@Configuration
public class InboundJmsAdaptorConfig {

	@Value("${sbt.dsl.jms.brokerUrl}")
	private String brokerUrl;

	@Value("${sbt.dsl.jms.username}")
	private String username;

	@Value("${sbt.dsl.jms.password}")
	private String password;

	@Value("${sbt.dsl.jms.queue}")
	private String queue;

	@Bean
	public ActiveMQConnectionFactory connectionFactory() {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
		connectionFactory.setBrokerURL(brokerUrl);
		connectionFactory.setPassword(password);
		connectionFactory.setUserName(username);
		return connectionFactory;
	}

	@Bean
	public JmsTemplate jmsTemplate() {
		JmsTemplate template = new JmsTemplate();
		template.setConnectionFactory(connectionFactory());
		return template;
	}

	@Autowired
	private JmsMessageHandler jmsMessageHandler;

	@Bean
	public IntegrationFlow amqpFlow() {

		return IntegrationFlows.from(Jms.inboundGateway(this.connectionFactory()).destination(queue))
				.handle(jmsMessageHandler, "process").channel("in-bound-request-channel").get();
	}

}
