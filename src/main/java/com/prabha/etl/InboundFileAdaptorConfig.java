package com.prabha.etl;

import java.io.File;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.file.DirectoryScanner;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.RecursiveDirectoryScanner;
import org.springframework.integration.file.filters.CompositeFileListFilter;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.transaction.PseudoTransactionManager;
import org.springframework.scheduling.annotation.EnableAsync;

/**
 * 
 * In-bound file-adaptor configuration to poll the file from the given directory
 * with interval configurable polling frequency.
 * 
 * @author Vadivel
 *
 */
@Configuration
@EnableAsync
public class InboundFileAdaptorConfig {

	@Autowired
	public File inboundIncomingDirectory;

	@Autowired
	public File inboundProcessingDirectory; 

	@Autowired
	private ETLFileHandler fileHandler;

	@Bean
	public IntegrationFlow inboundFileIntegration(TaskExecutor taskExecutor,
			MessageSource<File> fileReadingMessageSource) {
		return IntegrationFlows
				.from(fileReadingMessageSource,
						c -> c.poller(Pollers.fixedDelay(1000).taskExecutor(taskExecutor).maxMessagesPerPoll(2)))
				.handle(fileHandler, "handle").channel("inbound-channel").get();
	}

	@Bean
	public PseudoTransactionManager pseudoTransactionManager() {
		return new PseudoTransactionManager();
	}

	@Bean
	public FileReadingMessageSource fileReadingMessageSource(DirectoryScanner directoryScanner) {
		final FileReadingMessageSource source = new FileReadingMessageSource();
		source.setDirectory(this.inboundIncomingDirectory);
		source.setScanner(directoryScanner);
		source.setAutoCreateDirectory(true);
		return source;
	}

	@Bean
	public DirectoryScanner directoryScanner() {
		final DirectoryScanner scanner = new RecursiveDirectoryScanner();
		final CompositeFileListFilter<File> filters = new CompositeFileListFilter<>();
		filters.addFilter(new SimplePatternFileListFilter("*.txt"));
		scanner.setFilter(filters);
		return scanner;
	}

	@Bean(name = "inboundIncomingDirectory")
	public File inboundIncomingDirectory() {
		return makeDirectory("D:\\etl\\incoming");
	}

	@Bean(name = "inboundProcessingDirectory")
	public File inboundProcessingDirectory() {
		return makeDirectory("D:\\etl\\processing");
	}

	private File makeDirectory(String path) {
		final File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}
		return file;
	}
	
	
}