package com.legstar.avro.cob2avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Reading a z/OS file which records can be variable length.
 * <p/>
 * Records are expected to contain data only (no RDW).
 * <p/>
 * Without the record length info, the read operations are not guaranteed to
 * deliver an exact record.
 * <p/>
 * The contract is like this:
 * <ul>
 * <li>The caller maintains a buffer large enough to hold the largest record
 * <li>That same buffer is being passed as a parameter on all read operations
 * <li>On each subsequent read (after the first one), the caller informs us of
 * the number of bytes actually processed (the actual record length)
 * </ul>
 * This assumes that the caller has the means to determine the actual record
 * length.
 * <p/>
 * It is also important that the buffer being passed from read operation to read
 * operation contains the residual, unprocessed, data so that we can present it
 * on the next read.
 */
public class ZosVariableFileInputStream extends FileInputStream implements
        ZosFileInputStream {

    /** The last number of bytes read from the file. */
    private int _count;

    /** Number of bytes that were passed to the user but he did not process yet. */
    private int _residual;
    
    /** How many bytes of the original file were not read yet.*/
    private long remaining;

    /**
     * Create a z/OS file stream.
     * 
     * @param arg0 the underlying file name
     * @throws FileNotFoundException if file cannot be located
     */
    public ZosVariableFileInputStream(File arg0) throws FileNotFoundException {
        super(arg0);
        _count = 0;
        _residual = 0;
        this.remaining = arg0.length();
    }

    /**
     * Reads bytes from the underlying file.
     * 
     * @param b the buffer to be filled if possible. It should still contain
     *        what was read last time
     * @return the number of bytes available to process (-1 if file exhausted)
     * @throws IOException if reading fails
     */
    public int read(byte[] b) throws IOException {
        return read(b, 0);
    }

    /**
     * Reads bytes from the underlying file.
     * <p/>
     * We might have data read in excess last time that we still need to process
     * so this will push unprocessed bytes at the start of the buffer and then
     * try to fill the rest of the buffer with fresh data (if there is room
     * left).
     * 
     * @param b the buffer to be filled if possible. It should still contains
     *        what was read last time
     * @param processed the number of bytes that were processed following the
     *        previous read operation.
     * @return the number of bytes available to process (-1 if file exhausted)
     * @throws IOException if reading fails
     */
    public int read(byte[] b, final int processed) throws IOException {

        _residual = _residual + _count - processed;
        if (_residual == b.length) {
            _count = 0;
            return _residual;
        } else if (_residual > 0) {
            System.arraycopy(b, processed, b, 0, _residual);
        }

        _count = super.read(b, _residual, b.length - _residual);
        remaining -= _count;
        if (_count == -1) {
            if (_residual > 0) {
                _count = 0;
                return _residual;
            } else {
                return _count;
            }
        }
        return _residual + _count;
    }

    public long remaining() {
        return remaining;
    }

}
