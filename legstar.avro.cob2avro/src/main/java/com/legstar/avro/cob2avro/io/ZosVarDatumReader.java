package com.legstar.avro.cob2avro.io;

import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;

import com.legstar.base.context.CobolContext;
import com.legstar.base.context.EbcdicCobolContext;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.base.visitor.FromCobolChoiceStrategy;

/**
 * Reads a mainframe byte stream where records are variable size.
 * <p/>
 * In this case there is no field giving the actual size of the record. That
 * size will be known only after the caller processes the data.
 * <p/>
 * This assumes there is a maximum size though. So data is fetched to fill that
 * maximum size buffer. Then the entire buffer is returned to the caller. On the
 * next call, caller indicates how many bytes of the last buffer were actually
 * processed.
 * <p/>
 * Turns each mainframe record into an Avro specific class instance.
 * <p/>
 * Offers optional seek capability to position stream at start of a record (with
 * help of a custom {@link ZosRecordMatcher} to be provided).
 * 
 * @param <D>
 */
public class ZosVarDatumReader<D> extends AbstractZosDatumReader < D > {

    /** The last number of bytes read from the file. */
    private int lastCount;

    /** Number of bytes that were passed to the user but he did not process yet. */
    private int residual;

    public ZosVarDatumReader(InputStream inStream, long length,
            CobolComplexType cobolType, Schema schema) throws IOException {
        this(inStream, length, new EbcdicCobolContext(), cobolType, null,
                schema);
    }

    public ZosVarDatumReader(InputStream inStream, long length,
            CobolContext cobolContext, CobolComplexType cobolType, Schema schema)
            throws IOException {
        this(inStream, length, cobolContext, cobolType, null, schema);
    }

    public ZosVarDatumReader(InputStream inStream, long length,
            CobolContext cobolContext, CobolComplexType cobolType,
            FromCobolChoiceStrategy customChoiceStrategy, Schema schema)
            throws IOException {
        super(inStream, length, cobolContext, cobolType, customChoiceStrategy,
                schema);
    }

    /**
     * Reads a full record from the stream.
     * <p/>
     * We might have data read in excess last time that we still need to process
     * so this will push unprocessed bytes (residual) at the start of the buffer
     * and then try to fill the rest of the buffer with fresh data (if there is
     * room left).
     * 
     * @param hostBytes a buffer where to read the record
     * @param processed the number of bytes that were processed following the
     *            previous read operation.
     * @return the number of bytes read from the stream
     * @throws IOException
     */
    public int readRecord(byte[] hostBytes, final int processed)
            throws IOException {

        if (getBytesPrefetched() > 0) {
            // Data must be prefetched only once, before any reads
            if (residual > 0 || lastCount > 0 || processed > 0) {
                throw new IOException(
                        "Data was prefetched after the first read");
            }
            lastCount = getBytesPrefetched();
            setBytesPrefetched(0);
        }

        residual = residual + lastCount - processed;
        if (residual == hostBytes.length) {
            lastCount = 0;
            // Buffer is already filled
            return residual;
        } else if (residual > 0) {
            // Move residual at start of buffer
            System.arraycopy(hostBytes, processed, hostBytes, 0, residual);
        }

        // Fill the buffer
        lastCount = read(hostBytes, residual, hostBytes.length - residual);
        if (lastCount == -1) {
            if (residual > 0) {
                lastCount = 0;
                return residual;
            } else {
                return lastCount;
            }
        }
        return residual + lastCount;
    }

    public int hostBytesPrefixLen() {
        return 0;
    }

}
