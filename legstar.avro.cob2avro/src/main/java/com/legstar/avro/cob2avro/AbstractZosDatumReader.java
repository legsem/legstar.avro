package com.legstar.avro.cob2avro;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.legstar.base.context.CobolContext;
import com.legstar.base.finder.CobolTypeFinder;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.base.visitor.FromCobolChoiceStrategy;

/**
 * Reads a mainframe byte stream made of concatenated records.
 * <p/>
 * Turns each mainframe record into an Avro specific class instance.
 * <p/>
 * Offers optional seek capability to position stream at start of a record (with
 * help of a custom {@link ZosRecordMatcher} to be provided).
 * 
 * @param <D>
 */
public abstract class AbstractZosDatumReader<D> implements Iterator < D >,
        Iterable < D >, Closeable {

    private final byte[] hostBytes;

    /**
     * z/OS data stream.
     */
    private final InputStream inStream;

    /**
     * z/OS COBOL configuration parameters
     */
    private final CobolContext cobolContext;

    /**
     * a description of the input mainframe records
     */
    private final CobolComplexType cobolType;

    /**
     * Custom redefines alternative selector. Only needed when the incoming
     * record has redefines and the default strategy is not good enough.
     */
    private final FromCobolChoiceStrategy customChoiceStrategy;

    /**
     * the Avro schema of the output records
     */
    private final Schema schema;

    /** How many bytes of the original stream were not read yet */
    private long available;

    /** How many bytes of the original stream were already read */
    private long bytesRead;

    /**
     * Position in hostBytes where we should start filling data after some was
     * already prefetched (generally to perform record matching).
     */
    private int bytesPrefetched;

    /** Number of bytes in hostBytes that were last used to produce a datum. */
    private int lastProcessed;

    /**
     * Number of bytes in the incoming stream that were already used to produce
     * datums (might be smaller than bytesRead when we read ahead).
     */
    private long bytesProcessed;

    private static Logger log = LoggerFactory
            .getLogger(AbstractZosDatumReader.class);

    /**
     * Create a zos datum reader.
     * 
     * @param inStream the incoming z/OS data stream
     * @param length the total size of the stream
     * @param cobolContext z/OS COBOL configuration parameters
     * @param cobolType a description of the input mainframe records
     * @param customChoiceStrategy custom redefines alternative selector
     * @param schema the Avro schema of the output records
     * @throws IOException if reading fails
     */
    public AbstractZosDatumReader(InputStream inStream, long length,
            CobolContext cobolContext, CobolComplexType cobolType,
            FromCobolChoiceStrategy customChoiceStrategy, Schema schema)
            throws IOException {
        this.inStream = inStream;
        this.schema = schema;
        this.cobolContext = cobolContext;
        this.customChoiceStrategy = customChoiceStrategy;
        this.cobolType = cobolType;
        this.hostBytes = new byte[cobolType.getMaxBytesLen()
                + hostBytesPrefixLen()];
        this.available = length;
    }

    public Iterator < D > iterator() {
        return this;
    }

    public boolean hasNext() {
        return available - bytesProcessed > 0;
    }

    @SuppressWarnings("unchecked")
    public D next() {
        try {
            bytesRead += readRecord(hostBytes, lastProcessed);
            Cob2AvroConverter converter = new Cob2AvroConverter(cobolContext,
                    hostBytes, hostBytesPrefixLen(), customChoiceStrategy,
                    schema);
            converter.visit(cobolType);
            bytesProcessed += lastProcessed = converter.getLastPos();
            D specific = (D) SpecificData.get().deepCopy(schema,
                    converter.getResultObject());

            if (log.isDebugEnabled()) {
                log.debug("Avro record=" + specific.toString());
            }

            return specific;
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        }
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    public void close() throws IOException {
        if (null != inStream) {
            inStream.close();
        }
    }

    /**
     * Match incoming data with the start of record pattern.
     * <p/>
     * After the initial try, slides one byte at a time until the start of a
     * record is found.
     * 
     * @param recordMatcher matches the start of a record
     * @throws IOException typically if file does not contain a matching record
     *             start
     */
    public void seekRecordStart(CobolTypeFinder recordMatcher)
            throws IOException {
        int signatureLen = recordMatcher.getSignatureLen();
        if (signatureLen > hostBytes.length) {
            throw new IllegalArgumentException(
                    "The record matcher signature length is longer that the total record length");
        }
        int bytesRead = readFully(hostBytes, 0, signatureLen);
        if (bytesRead < signatureLen) {
            throw new IOException(
                    "Not enough bytes left for a record signature");
        }
        while (true) {
            if (recordMatcher.match(hostBytes, 0, signatureLen)) {
                break;
            }
            // Shift the buffer one byte to the left
            System.arraycopy(hostBytes, 1, hostBytes, 0, signatureLen - 1);

            // Read one byte at end of buffer
            bytesRead += readFully(hostBytes, signatureLen - 1, 1);
        }

        // Keep track of the record part that was read to match the signature
        setBytesPrefetched(signatureLen);

        // Keep tally of how many bytes were read
        this.bytesRead += bytesRead;

        // Consider all bytes before the found record to be already processed
        this.bytesProcessed = this.bytesRead - signatureLen;
    }

    /**
     * Reads a full record from the stream.
     * 
     * @param hostBytes a buffer where to read the record
     * @param processed the number of bytes in hostBytes that were processed
     *            following the previous read operation.
     * @return the number of bytes read from the stream
     * @throws IOException
     */
    public abstract int readRecord(byte[] hostBytes, int processed)
            throws IOException;

    /**
     * In the case where the host data is laid out with a fixed prefix before
     * the actual record data.
     * 
     * @return fixed prefix length before the actual record data
     */
    public abstract int hostBytesPrefixLen();

    /**
     * Read a number of bytes from the input stream, blocking until all
     * requested bytes are read or end of file is reached.
     * 
     * @param b the buffer to bill
     * @param off offset in buffer where to start filling
     * @param len how many bytes we should read
     * @return the total number of bytes read
     * @throws IOException if end of file reached without getting all requested
     *             bytes
     */
    public int readFully(byte b[], int off, int len) throws IOException {
        IOUtils.readFully(inStream, b, off, len);
        return len;
    }

    /**
     * Same as above but does not throw an exception at end of file, just
     * returns the actual data read.
     * 
     * @param b the buffer to bill
     * @param off offset in buffer where to start filling
     * @param len how many bytes we should read
     * @return the total number of bytes read
     * @throws IOException if a read error occurs
     */
    public int read(byte b[], int off, int len) throws IOException {
        return IOUtils.read(inStream, b, off, len);
    }

    public long getBytesRead() {
        return bytesRead;
    }

    public int getBytesPrefetched() {
        return bytesPrefetched;
    }

    public void setBytesPrefetched(int bytesPrefetched) {
        this.bytesPrefetched = bytesPrefetched;
    }

}
