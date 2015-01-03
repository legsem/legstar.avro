package com.legstar.avro.generator;


/**
 * Failed to translate a COBOL copybook to java converter support classes.
 *
 */
public class Cob2AvroGeneratotException extends RuntimeException {

    private static final long serialVersionUID = 8566746859018182572L;

    public Cob2AvroGeneratotException(final String message) {
        super(message);
    }

    public Cob2AvroGeneratotException(final String message, Throwable cause) {
        super(message, cause);
    }

    public Cob2AvroGeneratotException(Throwable cause) {
        super(cause);
    }
}
