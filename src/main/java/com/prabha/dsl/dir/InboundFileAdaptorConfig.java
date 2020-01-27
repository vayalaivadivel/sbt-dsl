package com.prabha.dsl.dir;

import java.io.File;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
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
	private ETLFileHandler fileHandler;

	@Value("${sbt.dsl.dir.incoming}")
	private String incoming;

	@Value("${sbt.dsl.dir.processing}")
	private String processing;

	@PostConstruct
	private void init() {
		makeDirectory(processing);
	}

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
		source.setDirectory(makeDirectory(incoming));
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
	
	private File makeDirectory(String path) {
		final File file = new File(path);
		if (!file.exists()) {
			file.mkdirs();
		}
		return file;
	}

}