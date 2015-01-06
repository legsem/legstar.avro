package com.legstar.avro.cob2avro;

import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

import com.legstar.base.FromHostResult;
import com.legstar.base.context.CobolContext;
import com.legstar.base.context.EbcdicCobolContext;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.base.visitor.FromCobolChoiceStrategy;

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
public class Cob2AvroGenericConverter {

    /**
     * Input COBOL type mapping to the target Avro record.
     */
    private final CobolComplexType cobolComplexType;

    /**
     * Target Avro Generic record schema.
     */
    private final Schema schema;

    /**
     * Parameters such as host character set.
     */
    private final CobolContext cobolContext;

    /**
     * Optionally, user might provide a custom strategy for alternative
     * selection.
     */
    private final FromCobolChoiceStrategy customChoiceStrategy;

    /**
     * Optionally, user might require variable values to be collected during
     * visiting.
     */
    private final Set < String > customVariables;

    public FromHostResult < GenericRecord > convert(byte[] hostData) {
        return convert(hostData, 0);
    }

    public FromHostResult < GenericRecord > convert(byte[] hostData, int start) {
        Cob2AvroVisitor visitor = new Cob2AvroVisitor(cobolContext, hostData,
                start, customChoiceStrategy, customVariables, schema);
        visitor.visit(cobolComplexType);
        return new FromHostResult < GenericRecord >(visitor.getLastPos(),
                (GenericRecord) visitor.getResultObject());
    }

    // -----------------------------------------------------------------------------
    // Builder section
    // -----------------------------------------------------------------------------
    public static class Builder {

        private CobolComplexType cobolComplexType;
        private Schema schema;
        private CobolContext cobolContext;
        private FromCobolChoiceStrategy customChoiceStrategy;
        private Set < String > customVariables;

        public Builder cobolComplexType(CobolComplexType cobolComplexType) {
            this.cobolComplexType = cobolComplexType;
            return this;
        }

        public Builder schema(Schema schema) {
            this.schema = schema;
            return this;
        }

        public Builder cobolContext(CobolContext cobolContext) {
            this.cobolContext = cobolContext;
            return this;
        }

        public Builder customChoiceStrategy(
                FromCobolChoiceStrategy customChoiceStrategy) {
            this.customChoiceStrategy = customChoiceStrategy;
            return this;
        }

        public Builder customVariables(Set < String > customVariables) {
            this.customVariables = customVariables;
            return this;
        }

        public Cob2AvroGenericConverter build() {
            return new Cob2AvroGenericConverter(this);
        }

    }

    // -----------------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------------
    private Cob2AvroGenericConverter(Builder builder) {
        cobolComplexType = builder.cobolComplexType;
        schema = builder.schema;
        cobolContext = builder.cobolContext == null ? new EbcdicCobolContext()
                : builder.cobolContext;
        customChoiceStrategy = builder.customChoiceStrategy;
        customVariables = builder.customVariables;
        if (cobolComplexType == null) {
            throw new IllegalArgumentException("You must provide a valid input CobolComplexType");
        }
        if (schema == null) {
            throw new IllegalArgumentException("You must provide a valid ouput Avro Schema");
        }
    }

}
