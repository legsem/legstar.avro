package com.legstar.avro.cob2avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecord;

import com.legstar.avro.cob2avro.Cob2AvroGenericConverter;
import com.legstar.base.converter.AbstractCob2ObjectConverter;
import com.legstar.base.converter.FromHostResult;
import com.legstar.base.visitor.Cob2ObjectValidator;

/**
 * Converts from host to an Avro specific record.
 * <p/>
 * There is a performance hit in converting to a specific record but these are
 * much easier to manipulate than the GenericRecord.
 * <p/>
 * In addition to converting this class offers methods to validate the content
 * of a host buffer against the proposed structure.
 * 
 */
public class Cob2AvroSpecificConverter extends
        AbstractCob2ObjectConverter < SpecificRecord > {

    /**
     * Maximum size in bytes for the host data corresponding to this avro
     * record.
     */
    private final int hostBytesLen;

    /** The avro schema for the record. */
    private final Schema schema;

    /** The underlying generic converter used. */
    private final Cob2AvroGenericConverter converter;

    /** The last record converted. */
    private SpecificRecord datum;

    public FromHostResult < SpecificRecord > convert(byte[] hostData,
            int start, int length) {
        FromHostResult < GenericRecord > result = converter.convert(hostData,
                start, length);
        if (result.getValue() == null) {
            return null;
        }
        this.datum = (SpecificRecord) SpecificData.get().deepCopy(
                result.getValue().getSchema(), result.getValue());
        return new FromHostResult < SpecificRecord >(
                result.getBytesProcessed(), datum);
    }

    public boolean isValid(byte[] hostData) {
        return isValid(hostData, 0, hostData.length);
    }

    public boolean isValid(byte[] hostData, int start, int length) {
        Cob2ObjectValidator validator = new Cob2ObjectValidator(
                getCobolContext(), hostData, start, length, null);
        validator.visit(getCobolComplexType());
        return validator.isValid();
    }

    public SpecificRecord getDatum() {
        return datum;
    }

    public int getHostBytesLen() {
        return hostBytesLen;
    }

    // -----------------------------------------------------------------------------
    // Builder section
    // -----------------------------------------------------------------------------
    public static class Builder extends
            AbstractCob2ObjectConverter.Builder < SpecificRecord, Builder > {

        private Schema schema;

        public Cob2AvroSpecificConverter build() {
            return new Cob2AvroSpecificConverter(this);
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        protected Builder self() {
            return this;
        }

    }

    // -----------------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------------
    private Cob2AvroSpecificConverter(Builder builder) {
        super(builder);
        schema = builder.schema;
        if (schema == null) {
            throw new IllegalArgumentException(
                    "You must provide a valid ouput Avro Schema");
        }
        converter = new Cob2AvroGenericConverter.Builder()
                .cobolContext(getCobolContext())
                .cobolComplexType(getCobolComplexType()).schema(schema).build();
        hostBytesLen = (int) getCobolComplexType().getMaxBytesLen();
    }

}
