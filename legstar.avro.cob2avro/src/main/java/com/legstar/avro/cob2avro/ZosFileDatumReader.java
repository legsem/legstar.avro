package com.legstar.avro.cob2avro;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Iterator;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;

import com.legstar.coxb.ICobolComplexBinding;
import com.legstar.coxb.host.HostException;

/**
 * Given a z/OS file on the local file system, which records are described by a
 * single COBOL copybook, iterates through the records delivering an avro
 * specific datum for each record.
 * 
 * @param <D> the Avro datum type
 */
public class ZosFileDatumReader<D> implements Iterator < D >, Iterable < D >,
        Closeable {

    private final ZosFileInputStream inStream;

    private final Schema schema;

    private final ICobolComplexBinding cobolBinding;

    private final byte[] hostBytes;

    private int processed;

    /**
     * Construct the reader.
     * 
     * @param zosFile the z/OS file on the local file system
     * @param hasRdw true if the file records start with a Record Descriptor
     *            Word (RDW)
     * @param schema the Avro schema for a record
     * @param cobolBinding the LegStar binding class for a record
     * @throws FileNotFoundException if z/OS file cannot be located
     */
    public ZosFileDatumReader(File zosFile, boolean hasRdw, Schema schema,
            ICobolComplexBinding cobolBinding) throws FileNotFoundException {
        this.inStream = hasRdw ? new ZosVariableRDWFileInputStream(zosFile)
                : new ZosVariableFileInputStream(zosFile);
        this.schema = schema;
        this.cobolBinding = cobolBinding;
        this.hostBytes = new byte[cobolBinding.getByteLength()];
    }

    public Iterator < D > iterator() {
        return this;
    }

    public boolean hasNext() {
        return inStream.remaining() > 0;
    }

    @SuppressWarnings("unchecked")
    public D next() {
        try {
            if ((inStream.read(hostBytes, processed)) <= 0) {
                throw new EOFException();
            }
            GenericRecord genericRecord = new GenericData.Record(schema);
            Cob2AvroUnmarshalVisitor visitor = new Cob2AvroUnmarshalVisitor(
                    hostBytes, genericRecord);
            visitor.visit(cobolBinding);
            processed = visitor.getOffset();
            return (D) SpecificData.get().deepCopy(schema, genericRecord);
        } catch (IOException e) {
            throw new AvroRuntimeException(e);
        } catch (HostException e) {
            throw new AvroRuntimeException(e);
        }
    }

    public void close() throws IOException {
        inStream.close();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

}