package com.legstar.avro.cob2avro;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

import com.legstar.coxb.convert.CobolConversionException;
import com.legstar.coxb.convert.simple.CobolBinarySimpleConverter;

/**
 * Reading a z/OS file which records can be variable length.
 * <p/>
 * Records are expected to start with a Record Descriptor Word (RDW).
 * This is a 4 bytes prefix which contains the length of the record.
 * <p/>
 * Here we use that length to read the exact number of bytes which
 * is optimal for variable length records.
 * <p/>
 * When downloading files from z/OS using FTP, you can preserve the RDW by issuing
 * the FTP command:
 * <pre>
 *      quote SITE RDW
 * </pre>
 */
public class ZosVariableRDWFileInputStream extends FileInputStream implements ZosFileInputStream {

    /** Work variable for RDW.*/
    private byte[] _rdw;

    /** How many bytes of the original file were not read yet.*/
    private long remaining;

    /**
     * Create a z/OS file stream.
     * 
     * @param arg0 the underlying file name
     * @throws FileNotFoundException if file cannot be located
     */
    public ZosVariableRDWFileInputStream(File arg0)
            throws FileNotFoundException {
        super(arg0);
        _rdw = new byte[4];
        this.remaining = arg0.length();
    }

    /**
     * Reads bytes from the underlying file.
     * 
     * @param b the buffer to be filled if possible. It must be large enough
     *            to accommodate the largest record
     * @return the number of bytes available to process (-1 if file exhausted)
     * @throws IOException if reading fails
     */
    public int read(byte[] b) throws IOException {
        int count = super.read(_rdw);
        if (count == -1) {
            return count;
        }
        if (count < _rdw.length) {
            throw new IOException("Record does not start with an RDW");
        }
        remaining -= count;
        try {
            int rdw = CobolBinarySimpleConverter.fromHostSingle(2, false, 4, 0,
                    _rdw, 0).intValue();
            if (rdw > 0) {
                /* Beware that raw rdw accounts for the rdw length (4 bytes) */
                rdw -= _rdw.length;
                if (rdw > b.length) {
                    throw new IOException(
                            "Record length extracted from RDW larger than maximum record length");
                }
                count = super.read(b, 0, rdw);
                remaining -= count;
                return count;
            } else {
                return 0;
            }
        } catch (CobolConversionException e) {
            throw new IOException("Unable to translate RDW content");
        }
    }

    /**
     * Reads bytes from the underlying file.
     * <p/>
     * This method ignores the number of bytes processed since the RDW
     * allows us to determine the exact length of a record.
     * 
     * @param b the buffer to be filled if possible. It must be large enough
     *            to accommodate the largest record
     * @param processed this parameter value is ignored.
     * @return the number of bytes available to process (-1 if file exhausted)
     * @throws IOException if reading fails
     */
    public int read(byte[] b, int processed) throws IOException {
        return read(b);
    }

    public long remaining() {
        return remaining;
    }

}
