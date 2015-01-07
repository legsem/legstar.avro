package com.legstar.avro.cob2avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.legstar.base.FromHostResult;
import com.legstar.base.converter.AbstractCob2ObjectConverter;
import com.legstar.base.type.composite.CobolComplexType;

/**
 * Converts mainframe data to an Avro Generic record.
 * <p/>
 * Convenience class using {@link Cob2AvroVisitor} to perform a record
 * conversion.
 * <p/>
 * Assumes a {@link CobolComplexType} and an Avro {@link Schema} are available.
 * The CobolComplexType describes the incoming mainframe datum and the Schema
 * describes the output Avro Generic record.
 * <p/>
 * This class is immutable and Thread safe.
 * 
 */
public class Cob2AvroGenericConverter extends AbstractCob2ObjectConverter < GenericRecord> {

    /**
     * Target Avro Generic record schema.
     */
    private final Schema schema;

    public FromHostResult < GenericRecord > convert(byte[] hostData, int start) {
        Cob2AvroVisitor visitor = new Cob2AvroVisitor(getCobolContext(), hostData,
                start, getCustomChoiceStrategy(), getCustomVariables(), schema);
        visitor.visit(getCobolComplexType());
        return new FromHostResult < GenericRecord >(visitor.getLastPos(),
                (GenericRecord) visitor.getResultObject());
    }

    // -----------------------------------------------------------------------------
    // Builder section
    // -----------------------------------------------------------------------------
    public static class Builder
            extends
            AbstractCob2ObjectConverter.Builder < GenericRecord, Builder > {

        private Schema schema;

        public Cob2AvroGenericConverter build() {
            return new Cob2AvroGenericConverter(this);
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
    private Cob2AvroGenericConverter(Builder builder) {
        super(builder);
        schema = builder.schema;
        if (schema == null) {
            throw new IllegalArgumentException("You must provide a valid ouput Avro Schema");
        }
    }

}
