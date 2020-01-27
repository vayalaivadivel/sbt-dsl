package com.prabha.dsl.util;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.prabha.dsl.dir.FileHandlerException;

/**
 * <p>
 * Common util/helper class which is used across all the modules for the below
 * purpose.
 * </p>
 * 
 * @author Vadivel
 *
 */
public class CommonUtils {

	public static final String DOT_STR = ".";
	public static final String UNDERSCORE_STR = "_";

	private static final Logger LOG = LoggerFactory.getLogger(CommonUtils.class);
	private static final DateFormat FORMATTER_WITH_TIME_STAMP_SECONDS = new SimpleDateFormat("yyyyMMdd'_'HHmmss");
	public static final String INBOUND_CHANNEL = "inbound-channel";

	
	public static String moveFile(final String file, final String destinationDir) {

		try {

			final StringBuilder fileNameBuilder = new StringBuilder(destinationDir);
			fileNameBuilder.append(File.separator);
			fileNameBuilder.append(StringUtils.substringAfterLast(file, File.separator));
			final String destinationFileName = fileNameBuilder.toString();

			Path temp = Files.move(Paths.get(file), Paths.get(destinationFileName),
					StandardCopyOption.REPLACE_EXISTING);
			LOG.info("File {} moved to destination directory {} ", file, destinationFileName);
			if (temp != null) {
				LOG.info("File moved successfully to {}", destinationDir);
			} else {
				LOG.info("Failed move the file to {}", destinationDir);
			}
			return destinationFileName;
		} catch (final IOException e) {
			final String errorMessage = String.format("Unable to move %s to destination directory %s due to : %s", file,
					destinationDir, e);
			LOG.error(errorMessage);

			throw new FileHandlerException(errorMessage);
		}
	}

	public static String constructFileNameWithTimeStamp(final String fileName, final Date time) {
		final StringBuilder builder = new StringBuilder(UNDERSCORE_STR);
		builder.append(FORMATTER_WITH_TIME_STAMP_SECONDS.format(time));
		builder.append(DOT_STR);
		final String fileNameWithTimeStamp = StringUtils.replace(fileName, DOT_STR, builder.toString());
		return fileNameWithTimeStamp;
	}

	public static String renameFile(final File oldFile, final String newFile) {
		final File newFileObj = new File(newFile);
		if (!oldFile.renameTo(newFileObj)) {
			LOG.error("Unable to rename the file...{}", newFile);
			throw new FileHandlerException("Unable to rename the file: " + newFile);
		}

		return newFile;
	}

	public static String convertObjectToJsonStr(final Object object) {
		try {
			return new ObjectMapper().writer().withDefaultPrettyPrinter().writeValueAsString(object);
		} catch (final JsonProcessingException e) {
			return null;
		}
	}

	public static void writeErrorDetails(final StringBuilder errorMessageBuilder, final String fileName,
			final String destinationDir) {
		LOG.info("fileName: {} and destinationDir");
		if (errorMessageBuilder.length() != 0) {
			final String errFileName = StringUtils.replace(fileName, ".xlsx", ".err");
			try (FileOutputStream outputStream = new FileOutputStream(errFileName)) {
				byte[] strToBytes = errorMessageBuilder.toString().getBytes();
				outputStream.write(strToBytes);
			} catch (Exception e) {
				LOG.error("Unable to write file due to {}", e);
			}
			moveFile(errFileName, destinationDir);
		}

	}

	public static void writeContent(final String fileName, final String content) {
		try {
			Files.write(Paths.get(fileName), content.getBytes());
		} catch (IOException e) {
			final String message = String.format("Unable to write content on the file %s", fileName);
			LOG.equals(message);
			throw new FileHandlerException(message);
		}

	}

}
