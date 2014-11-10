package com.legstar.avro.cob2avro;

import java.io.IOException;

/**
 * A set of methods to read binary data from a z/OS file.
 * 
 */
public interface ZosFileInputStream {

    /**
     * Reads bytes from the underlying file.
     * 
     * @param b the buffer to be filled if possible. It should still contain
     *            what was read last time
     * @return the number of bytes available to process (-1 if file exhausted)
     * @throws IOException if reading fails
     */
    int read(byte[] b) throws IOException;

    /**
     * Reads bytes from the underlying file.
     * <p/>
     * We might have data read in excess last time that we still need to process
     * so this will push unprocessed bytes at the start of the buffer and then
     * try to fill the rest of the buffer with fresh data (if there is room
     * left).
     * 
     * @param b the buffer to be filled if possible. It should still contains
     *            what was read last time
     * @param processed the number of bytes that were processed following the
     *            previous read operation.
     * @return the number of bytes available to process (-1 if file exhausted)
     * @throws IOException if reading fails
     */
    int read(byte[] b, final int processed) throws IOException;

    /**
     * We are done with the stream, close the underlying file.
     * 
     * @throws IOException if close fails
     */
    public void close() throws IOException;

    /**
     * How many bytes of the original file were not read yet.
     * 
     * @return how many bytes of the original file were not read yet
     */
    long remaining();
}
