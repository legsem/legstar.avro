package com.legstar.avro.cob2avro;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import com.legstar.base.ConversionException;
import com.legstar.base.context.CobolContext;
import com.legstar.base.type.CobolType;
import com.legstar.base.type.composite.CobolArrayType;
import com.legstar.base.type.composite.CobolChoiceType;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.base.type.primitive.CobolPrimitiveType;
import com.legstar.base.visitor.FromCobolChoiceStrategy;
import com.legstar.base.visitor.FromCobolVisitor;

public class Cob2AvroVisitor extends FromCobolVisitor {

    /** Holds the current avro schema during the course of visiting fields.*/
    private Schema currentSchema;

    /** Last avro object produced by visiting a field. */
    private Object resultObject;

    /**
     * Set of unique handlers to receive notifications from
     * {@link FromCobolVisitor}
     */
    private final AvroPrimitiveTypeHandler primitiveTypeHandler;
    private final AvroChoiceTypeAlternativeHandler choiceTypeHandler;

    // -----------------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------------
    public Cob2AvroVisitor(CobolContext cobolContext, byte[] hostData,
            int start, Schema schema) {
        this(cobolContext, hostData, start, null, schema);
    }

    public Cob2AvroVisitor(CobolContext cobolContext, byte[] hostData,
            int start, FromCobolChoiceStrategy customChoiceStrategy,
            Schema schema) {
        this(cobolContext, hostData, start, customChoiceStrategy, null, schema);
    }

    public Cob2AvroVisitor(CobolContext cobolContext, byte[] hostData,
            int start, FromCobolChoiceStrategy customChoiceStrategy,
            Set < String > customVariables, Schema schema) {
        super(cobolContext, hostData, start, customChoiceStrategy,
                customVariables);
        currentSchema = schema;
        primitiveTypeHandler = new AvroPrimitiveTypeHandler();
        choiceTypeHandler = new AvroChoiceTypeAlternativeHandler();
    }

    // -----------------------------------------------------------------------------
    // Visit methods
    // -----------------------------------------------------------------------------
    public void visit(CobolComplexType type) throws ConversionException {
        GenericRecord record = new GenericData.Record(currentSchema);
        super.visitComplexType(type, new AvroComplexTypeChildHandler(record));
        resultObject = record;
    }

    public void visit(CobolArrayType type) throws ConversionException {
        final List < Object > list = new ArrayList < Object >();
        super.visitCobolArrayType(type, new AvroArrayTypeItemHandler(list));
        resultObject = list;
    }

    public void visit(CobolChoiceType type) throws ConversionException {
        super.visitCobolChoiceType(type, choiceTypeHandler);
    }

    public void visit(CobolPrimitiveType < ? > type) throws ConversionException {
        super.visitCobolPrimitiveType(type, primitiveTypeHandler);
    }

    // -----------------------------------------------------------------------------
    // Handlers
    // -----------------------------------------------------------------------------
    private class AvroComplexTypeChildHandler implements
            ComplexTypeChildHandler {

        private final GenericRecord record;

        private Schema previousSchema;

        public AvroComplexTypeChildHandler(GenericRecord record) {
            this.record = record;
        }

        public boolean preVisit(String fieldName, int fieldIndex,
                CobolType child) {

            // Create the Avro schema context for the child
            previousSchema = currentSchema;
            if (child instanceof CobolComplexType) {
                Schema childSchema = record.getSchema().getField(fieldName).schema();
                // Optional fields are unions between a record type and "null"
                if (childSchema.getType().equals(Schema.Type.UNION)) {
                    currentSchema = childSchema.getTypes().get(0);
                } else {
                    currentSchema = record.getSchema().getField(fieldName).schema();
                }
            } else if (child instanceof CobolArrayType) {
                currentSchema = record.getSchema().getField(fieldName).schema()
                        .getElementType();
            } else if (child instanceof CobolChoiceType) {
                currentSchema = record.getSchema().getField(fieldName).schema();
            }
            return true;
        }

        public boolean postVisit(String fieldName, int fieldIndex,
                CobolType child) {

            record.put(fieldName, resultObject);

            // Restore the Avro schema context
            currentSchema = previousSchema;
            return true;
        }

    }

    private class AvroArrayTypeItemHandler implements ArrayTypeItemHandler {

        private final List < Object > list;

        public AvroArrayTypeItemHandler(List < Object > list) {
            this.list = list;
        }

        public boolean preVisit(int itemIndex, CobolType item) {
            return true;
        }

        public boolean postVisit(int itemIndex, CobolType item) {
            list.add(resultObject);
            return true;
        }

    }

    private class AvroChoiceTypeAlternativeHandler implements
            ChoiceTypeAlternativeHandler {

        private Schema previousSchema;

        public void preVisit(String alternativeName, int alternativeIndex,
                CobolType alternative) {
            // Set the alternative schema as current
            previousSchema = currentSchema;
            currentSchema = currentSchema.getTypes().get(alternativeIndex);

        }

        public void postVisit(String alternativeName, int alternativeIndex,
                CobolType alternative) {
            // Restore the Avro schema context
            currentSchema = previousSchema;
        }

    }

    private class AvroPrimitiveTypeHandler implements PrimitiveTypeHandler {

        public void preVisit(CobolPrimitiveType < ? > type) {

        }

        public void postVisit(CobolPrimitiveType < ? > type, Object value) {
            if (value instanceof BigDecimal) {
                resultObject = DecimalUtils.toByteBuffer((BigDecimal) value);
            } else if (value instanceof BigInteger) {
                // TODO there is a risk of overflow here but Avro does not have
                // unsigned int/long
                resultObject = ((BigInteger) value).longValue();
            } else {
                resultObject = value;
            }
        }

    }

    // -----------------------------------------------------------------------------
    // Getters
    // -----------------------------------------------------------------------------
    public Object getResultObject() {
        return resultObject;
    }
}
