package com.prabha.etl;

import java.io.File;
import java.util.Date;

import org.springframework.stereotype.Component;

@Component
public class ETLFileHandler {

	public void handle(final File file) {
		final String fileName = file.getName();
		System.out.println("--------File Received----------" + fileName);
		final String newFileName = moveAndConstructNewFile(file);
		System.out.println("--------File Moved to process directory----------" + newFileName);
	}

	private String moveAndConstructNewFile(final File file) {
		final Date batchDateTime = new Date();
		final String originalFilePath = file.getAbsolutePath();
		String destFile = DMUtils.moveFile(originalFilePath, "D:\\etl\\processing");
		System.out.println("--------File {}--------" + destFile);

		final String fileNameWithTimeStamp = DMUtils.constructFileNameWithTimeStamp(destFile, batchDateTime);
		DMUtils.renameFile(new File(destFile), fileNameWithTimeStamp);
		return fileNameWithTimeStamp;
	}

}
