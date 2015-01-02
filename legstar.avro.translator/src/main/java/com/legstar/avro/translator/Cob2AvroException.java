package com.legstar.avro.translator;


/**
 * Failed to translate XML schema to Avro Schema.
 *
 */
public class Cob2AvroException extends RuntimeException {

	private static final long serialVersionUID = -9123916734116875812L;

	public Cob2AvroException(final String message) {
		super(message);
	}

	public Cob2AvroException(final String message, Throwable cause) {
		super(message, cause);
	}

	public Cob2AvroException(Throwable cause) {
		super(cause);
	}

}
