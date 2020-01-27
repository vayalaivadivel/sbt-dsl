package com.prabha.dsl.dir;

import java.io.File;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.prabha.dsl.util.CommonUtils;

@Component
public class ETLFileHandler {

	private static final Logger LOG = LoggerFactory.getLogger(ETLFileHandler.class);

	public void handle(final File file) {
		final String fileName = file.getName();
		LOG.info("--------File Received----------" + fileName);
		final String newFileName = moveAndConstructNewFile(file);
		LOG.info("--------File Moved to process directory----------" + newFileName);
	}

	private String moveAndConstructNewFile(final File file) {
		final Date batchDateTime = new Date();
		final String originalFilePath = file.getAbsolutePath();
		String destFile = CommonUtils.moveFile(originalFilePath, "D:\\etl\\processing");
		System.out.println("--------File {}--------" + destFile);

		final String fileNameWithTimeStamp = CommonUtils.constructFileNameWithTimeStamp(destFile, batchDateTime);
		CommonUtils.renameFile(new File(destFile), fileNameWithTimeStamp);
		return fileNameWithTimeStamp;
	}

}
