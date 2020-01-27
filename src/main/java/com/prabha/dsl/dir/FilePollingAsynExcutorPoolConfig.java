package com.prabha.dsl.dir;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.AsyncConfigurerSupport;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

/**
 * File Polling frequency executor Pool configuration
 * 
 * @author Vadivel
 *
 */
@Configuration
@EnableAsync
public class FilePollingAsynExcutorPoolConfig extends AsyncConfigurerSupport {
	@Bean
	@Primary
	public TaskExecutor threadPoolTaskExecutor() {

		ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
		executor.setCorePoolSize(4);
		executor.setMaxPoolSize(4);
		executor.setThreadNamePrefix("Sample Pooling Thread");
		executor.initialize();

		return executor;
	}
}
