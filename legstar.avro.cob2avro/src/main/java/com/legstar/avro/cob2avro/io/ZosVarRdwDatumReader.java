package com.legstar.avro.cob2avro.io;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.avro.Schema;

import com.legstar.base.context.CobolContext;
import com.legstar.base.context.EbcdicCobolContext;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.base.visitor.FromCobolChoiceStrategy;

/**
 * Reads a mainframe byte stream where records are variable size and start with
 * an RDW (Record Descriptor Word).
 * <p/>
 * Turns each mainframe record into an Avro specific class instance.
 * <p/>
 * Offers optional seek capability to position stream at start of a record (with
 * help of a custom {@link ZosRecordMatcher} to be provided).
 * 
 * @param <D>
 */
public class ZosVarRdwDatumReader<D> extends AbstractZosDatumReader < D > {

    public static final int RDW_LEN = 4;

    public ZosVarRdwDatumReader(InputStream inStream, long length,
            CobolComplexType cobolType, Schema schema) throws IOException {
        this(inStream, length, new EbcdicCobolContext(), cobolType, null,
                schema);
    }

    public ZosVarRdwDatumReader(InputStream inStream, long length,
            CobolContext cobolContext, CobolComplexType cobolType, Schema schema)
            throws IOException {
        this(inStream, length, cobolContext, cobolType, null, schema);
    }

    public ZosVarRdwDatumReader(InputStream inStream, long length,
            CobolContext cobolContext, CobolComplexType cobolType,
            FromCobolChoiceStrategy customChoiceStrategy, Schema schema)
            throws IOException {
        super(inStream, length, cobolContext, cobolType, customChoiceStrategy, schema);
    }

    /**
     * Reads a full record from the stream.
     * <p/>
     * A positive bytesPrefetched signals that part of the record was already
     * read previously in which case we just read the complement.
     * <p/>
     * Otherwise we start by reading the rdw and the actual record.
     * 
     * @param hostBytes a buffer where to read the record
     * @return the number of bytes read from the stream
     * @throws IOException
     */
    public int readRecord(byte[] hostBytes, int processed) throws IOException {
        int bytesRead = 0;
        if (getBytesPrefetched() == 0) {
            bytesRead = readFully(hostBytes, 0, RDW_LEN);
            if (bytesRead < RDW_LEN) {
                throw new IOException(
                        "Not enough bytes left for a record descriptor word");
            }
            setBytesPrefetched(RDW_LEN);
        }
        int recordLen = getRecordLen(hostBytes);
        bytesRead += readFully(hostBytes, getBytesPrefetched(), recordLen
                - getBytesPrefetched() + RDW_LEN);

        setBytesPrefetched(0);
        return bytesRead;
    }

    /**
     * RDW is a 4 bytes numeric stored in Big Endian as a binary 2's complement.
     * 
     * @param hostData the mainframe data
     * @return the size of the record (actual data without the rdw itself)
     */
    private static int getRecordLen(byte[] hostData) {
        return getRecordLen(hostData, 0, hostData.length);
    }

    /**
     * RDW is a 4 bytes numeric stored in Big Endian as a binary 2's complement.
     * 
     * @param hostData the mainframe data
     * @param start where the RDW starts
     * @param length the total size of the mainframe data
     * @return the size of the record (actual data without the rdw itself)
     */
    private static int getRecordLen(byte[] hostData, int start, int length) {

        int len = getRawRdw(hostData, start, length);
        if (len < RDW_LEN || len > hostData.length) {
            throw new IllegalArgumentException(
                    "Record does not start with a Record Descriptor Word");
        }
        /* Beware that raw rdw accounts for the rdw length (4 bytes) */
        return len - RDW_LEN;
    }

    /**
     * RDW is a 4 bytes numeric where the first 2 bytes are the length of the
     * record (LL) including the 4 byte RDW.
     * 
     * @param hostData the mainframe data
     * @param start where the RDW starts
     * @param length the total size of the mainframe data
     * @return the integer content of the RDW
     */
    public static int getRawRdw(byte[] hostData, int start, int length) {
        if (length - start < RDW_LEN) {
            throw new IllegalArgumentException("Not enough bytes for an RDW");
        }
        ByteBuffer buf = ByteBuffer.allocate(RDW_LEN);
        buf.put(0, (byte) 0);
        buf.put(1, (byte) 0);
        buf.put(2, hostData[start + 0]);
        buf.put(3, hostData[start + 1]);

        return buf.getInt();
    }

    public int hostBytesPrefixLen() {
        return RDW_LEN;
    }

}
