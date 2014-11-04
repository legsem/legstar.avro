package com.legstar.avro.translator;

import java.io.IOException;

public class Cob2AvroException extends IOException {

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
