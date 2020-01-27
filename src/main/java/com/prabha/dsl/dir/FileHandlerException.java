package com.prabha.dsl.dir;

/**
 * Thrown when the exception caught during the file processing
 * 
 * @author Vadivel
 *
 */
public class FileHandlerException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public FileHandlerException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public FileHandlerException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

}
