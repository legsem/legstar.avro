package com.legstar.avro.translator;


/**
 * Failed to translate XML schema to Avro Schema.
 *
 */
public class Xsd2AvroTranslatorException extends RuntimeException {

	private static final long serialVersionUID = -9123916734116875812L;

	public Xsd2AvroTranslatorException(final String message) {
		super(message);
	}

	public Xsd2AvroTranslatorException(final String message, Throwable cause) {
		super(message, cause);
	}

	public Xsd2AvroTranslatorException(Throwable cause) {
		super(cause);
	}

}
