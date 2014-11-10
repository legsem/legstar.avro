package com.legstar.avro.cob2avro;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.legstar.coxb.CobolElementVisitor;
import com.legstar.coxb.ICobolArrayBinaryBinding;
import com.legstar.coxb.ICobolArrayBinding;
import com.legstar.coxb.ICobolArrayComplexBinding;
import com.legstar.coxb.ICobolArrayDbcsBinding;
import com.legstar.coxb.ICobolArrayDoubleBinding;
import com.legstar.coxb.ICobolArrayFloatBinding;
import com.legstar.coxb.ICobolArrayNationalBinding;
import com.legstar.coxb.ICobolArrayNumericBinding;
import com.legstar.coxb.ICobolArrayOctetStreamBinding;
import com.legstar.coxb.ICobolArrayPackedDecimalBinding;
import com.legstar.coxb.ICobolArrayStringBinding;
import com.legstar.coxb.ICobolArrayZonedDecimalBinding;
import com.legstar.coxb.ICobolBinaryBinding;
import com.legstar.coxb.ICobolBinding;
import com.legstar.coxb.ICobolChoiceBinding;
import com.legstar.coxb.ICobolComplexBinding;
import com.legstar.coxb.ICobolDbcsBinding;
import com.legstar.coxb.ICobolDoubleBinding;
import com.legstar.coxb.ICobolFloatBinding;
import com.legstar.coxb.ICobolNationalBinding;
import com.legstar.coxb.ICobolNumericBinding;
import com.legstar.coxb.ICobolOctetStreamBinding;
import com.legstar.coxb.ICobolPackedDecimalBinding;
import com.legstar.coxb.ICobolStringBinding;
import com.legstar.coxb.ICobolZonedDecimalBinding;
import com.legstar.coxb.convert.ICobolConverters;
import com.legstar.coxb.convert.simple.CobolSimpleConverters;
import com.legstar.coxb.host.HostException;
import com.legstar.coxb.util.NameUtil;

/**
 * Unmarshal mainframe data to an avro generic record.
 * <p/>
 * This class in NOT thread safe. 
 */
public class Cob2AvroUnmarshalVisitor extends CobolElementVisitor {

    private static Logger log = LoggerFactory
            .getLogger(Cob2AvroUnmarshalVisitor.class);

    /** Outcome of the unmarshaller. */
    private final GenericRecord rootAvroRecord;

    private final CobolZonedDecimalConverter cobolZonedDecimalConverter = new CobolZonedDecimalConverter();
    private final CobolPackedDecimalConverter cobolPackedDecimalConverter = new CobolPackedDecimalConverter();
    private final CobolBinaryConverter cobolBinaryConverter = new CobolBinaryConverter();
    private final CobolStringConverter cobolStringConverter = new CobolStringConverter();
    private final CobolNationalConverter cobolNationalConverter = new CobolNationalConverter();
    private final CobolDbcsConverter cobolDbcsConverter = new CobolDbcsConverter();
    private final CobolFloatConverter cobolFloatConverter = new CobolFloatConverter();
    private final CobolDoubleConverter cobolDoubleConverter = new CobolDoubleConverter();
    private final CobolOctetStreamConverter cobolOctetStreamConverter = new CobolOctetStreamConverter();

    private final CobolArrayZonedDecimalConverter cobolArrayZonedDecimalConverter = new CobolArrayZonedDecimalConverter();
    private final CobolArrayPackedDecimalConverter cobolArrayPackedDecimalConverter = new CobolArrayPackedDecimalConverter();
    private final CobolArrayBinaryConverter cobolArrayBinaryConverter = new CobolArrayBinaryConverter();
    private final CobolArrayStringConverter cobolArrayStringConverter = new CobolArrayStringConverter();
    private final CobolArrayNationalConverter cobolArrayNationalConverter = new CobolArrayNationalConverter();
    private final CobolArrayDbcsConverter cobolArrayDbcsConverter = new CobolArrayDbcsConverter();
    private final CobolArrayFloatConverter cobolArrayFloatConverter = new CobolArrayFloatConverter();
    private final CobolArrayDoubleConverter cobolArrayDoubleConverter = new CobolArrayDoubleConverter();
    private final CobolArrayOctetStreamConverter cobolArrayOctetStreamConverter = new CobolArrayOctetStreamConverter();

    /** Current record being unmarshalled. */
    private GenericRecord currentAvroRecord;
    
    /** Current avro choice (union) field name. */
    private String currentAvroChoiceFieldName;
    
    /** Selected alternative schema. */
    private Schema currentAvroChoiceAlternativeSchema;

    /**
     * Visitor constructor.
     * 
     * @param hostBytes host buffer used by visitor
     */
    public Cob2AvroUnmarshalVisitor(final byte[] hostBytes,
            GenericRecord avroRecord) {
        this(hostBytes, 0, avroRecord, new CobolSimpleConverters());
    }

    /**
     * Visitor constructor.
     * 
     * @param hostBytes host buffer used by visitor
     * @param cobolConverters set of converters to use for cobol elements
     */
    public Cob2AvroUnmarshalVisitor(final byte[] hostBytes,
            GenericRecord avroRecord, final ICobolConverters cobolConverters) {
        this(hostBytes, 0, avroRecord, cobolConverters);
    }

    /**
     * Visitor constructor.
     * 
     * @param hostBytes host buffer used by visitor
     * @param offset offset in host buffer
     * @param cobolConverters set of converters to use for cobol elements
     */
    public Cob2AvroUnmarshalVisitor(final byte[] hostBytes, final int offset,
            GenericRecord avroRecord, final ICobolConverters cobolConverters) {
        super(hostBytes, offset, cobolConverters);
        this.rootAvroRecord = avroRecord;
        currentAvroRecord = rootAvroRecord;
    }

    public void visit(ICobolComplexBinding ce) throws HostException {

        /* Object might be optional. Check if it should be visited. */
        if (!exists(ce)) {
            return;
        }

        /*
         * Ask complex binding to create an empty bound value object ready for
         * unmarshaling.
         */
        ce.createValueObject();

        GenericRecord avroRecord = currentAvroRecord;
        log.debug("Start visiting complex type {} in avro schema {}",
                ce.getJaxbName(), avroRecord.getSchema());

        int index = 0;
        for (ICobolBinding child : ce.getChildrenList()) {

            if (child instanceof ICobolComplexBinding) {

                String avroFieldName = getAvroFieldName(child);
                Schema avroSchema = getSchema(avroRecord, avroFieldName);

                log.debug("Start visiting field {} with avro schema {}",
                        avroFieldName, avroSchema);

                currentAvroRecord = new GenericData.Record(avroSchema);
                avroRecord.put(avroFieldName, currentAvroRecord);
                child.accept(this);
                // restore current record to what is was before processing child
                currentAvroRecord = avroRecord;
                log.debug("Restored avro record to " + avroRecord.getSchema());

                log.debug("End visiting field {} with avro schema {} value={}",
                        avroFieldName, avroSchema,
                        child.getObjectValue(child.getJaxbType()));

            } else {
                child.accept(this);
            }
            ce.setPropertyValue(index++);


        }

        log.debug("End visiting complex type {} in avro schema {}",
                ce.getJaxbName(), avroRecord.getSchema());

    }

    @Override
    public void visit(ICobolChoiceBinding ce) throws HostException {

        log.debug("Start visiting choice type {}", ce.getBindingName());

        /*
         * Make sure there are no leftovers from a previous use of this binding
         * and evaluate the maximum alternative length.
         */
        String firstAlternativeName = null;
        int maxAlternaliveLength = 0;
        for (ICobolBinding alternative : ce.getAlternativesList()) {
            String alternativeName = getAvroFieldName(alternative);
            if (firstAlternativeName == null) {
                firstAlternativeName = alternativeName;
            }
            alternative.setObjectValue(null);
            if (alternative.getByteLength() > maxAlternaliveLength) {
                maxAlternaliveLength = alternative.getByteLength();
            }
        }

        String previousAvroChoiceFieldName = currentAvroChoiceFieldName;
        currentAvroChoiceFieldName = firstAlternativeName + "Choice";

        /*
         * In a choice situation, only one alternative should be accepted when
         * this element is visited. The logic to determine which alternative
         * should be selected is customizable via the
         * ICobolUnmarshalChoiceStrategy interface. If no direct selection of an
         * alternative is available, the default behavior is to select the first
         * alternative that is accepted without error and moved the offset.
         */
        ICobolBinding chosenAlternative = null;

        /* If an external selector is provided, try it first */
        if (ce.getUnmarshalChoiceStrategy() != null) {

            log.debug("Calling Unmarshal choice strategy {} ", ce.getUnmarshalChoiceStrategyClassName());

            chosenAlternative = ce.getUnmarshalChoiceStrategy().choose(ce,
                    getVariablesMap(), this);
            /* If selector was successful, use the selected alternative */
            if (chosenAlternative != null) {
                visitAlternative(ce.getAlternativesList(), chosenAlternative);
            }
        }

        /* Default behavior if direct selection was not possible */
        if (chosenAlternative == null) {
            int index = 0;
            for (ICobolBinding alt : ce.getAlternativesList()) {
                /* Save the visitor offset context */
                int savedOffset = getStartOffset();
                try {
                    visitAlternative(index, alt);
                    /*
                     * When unmarshaling, a non present alternative is expected
                     * to result in an exception. Otherwise, we consider we
                     * found a valid alternative, just get out of the loop.
                     */
                    chosenAlternative = alt;
                    break;
                } catch (HostException he) {
                    setOffset(savedOffset);
                }
                index++;
            }
        }

        /* If none of the alternatives worked, raise an exception */
        if (chosenAlternative == null) {
            throw new HostException("No alternative found for choice element "
                    + ce.getBindingName());
        }

        /*
         * Ask choice binding to set bound object value from unmarshaled
         * data.
         */
        ce.setPropertyValue(ce.getAlternativesList().indexOf(
                chosenAlternative));
        /*
         * If chosen alternative is shorter than the max, keep record of the
         * difference because next item is not variably located.
         */
        if (chosenAlternative.getByteLength() < maxAlternaliveLength) {
            setVirtualFillerLength(maxAlternaliveLength
                    - chosenAlternative.getByteLength());
        }
        
        // Cleanup
        currentAvroChoiceFieldName = previousAvroChoiceFieldName;
        

        log.debug("End visiting choice type {}", ce.getBindingName());
    }
    
    /**
     * Visit an alternative from a choice.
     * <p/>
     * If the alternative is a record, set the context so that children get
     * unmarshaled to the alternative (rather tha the parent avro record).
     * 
     * @param alternativesList the alternatives list
     * @param alt the alternative to be visited
     * @param avroChoiceSchema the choice's avro schema
     * @throws HostException
     */
    private void visitAlternative(
            List < ICobolBinding > alternativesList, ICobolBinding alt) throws HostException {
        visitAlternative(alternativesList.indexOf(alt), alt);
    }

    /**
     * Visit an alternative from a choice.
     * <p/>
     * If the alternative is a record, set the context so that children get
     * unmarshaled to the alternative (rather tha the parent avro record).
     * 
     * @param altIndex index of the alternative to be visited in the alternatives list
     * @param alt the alternative to be visited
     * @throws HostException
     */
    private void visitAlternative(
            int altIndex, ICobolBinding alt) throws HostException {
        GenericRecord avroRecord = currentAvroRecord;
        Schema avroChoiceAlternativeSchema = currentAvroChoiceAlternativeSchema;
        Schema avroChoiceSchema = getSchema(avroRecord, currentAvroChoiceFieldName);
        try {
            String avroTypeName = getAvroTypeName(alt);
            currentAvroChoiceAlternativeSchema = avroChoiceSchema.getTypes().get(altIndex);
            if (currentAvroChoiceAlternativeSchema.getType() == Schema.Type.RECORD) {
                currentAvroRecord = new GenericData.Record(currentAvroChoiceAlternativeSchema);
                putValue(avroRecord, avroTypeName, currentAvroRecord);
            }
            alt.accept(this);
        } finally {
            currentAvroRecord = avroRecord;
            currentAvroChoiceAlternativeSchema = avroChoiceAlternativeSchema;
        }
    }

    @Override
    public void visit(ICobolArrayComplexBinding ce) throws HostException {

        /*
         * Ask complex array binding to initialize bound array so that it is
         * ready for unmarshaling.
         */
        ce.createValueObject();

        GenericRecord avroParentRecord = currentAvroRecord;

        String avroFieldName = getAvroFieldName(ce);
        Schema avroArraySchema = getSchema(avroParentRecord, avroFieldName);
        GenericArray < GenericRecord > avroArray = new GenericData.Array < GenericRecord >(
                ce.getCurrentOccurs(), avroArraySchema);

        log.debug(
                "Start visiting complex array type {} with {} items in avro schema {}",
                ce.getJaxbName(), ce.getCurrentOccurs(), avroArray.getSchema());

        Schema itemSchema = avroArray.getSchema().getElementType();
        ICobolComplexBinding complexItem = ce.getComplexItemBinding();

        for (int i = 0; i < ce.getCurrentOccurs(); i++) {
            currentAvroRecord = new GenericData.Record(itemSchema);
            complexItem.accept(this);
            avroArray.add(currentAvroRecord);
            ce.addPropertyValue(i);
        }

        avroParentRecord.put(avroFieldName, avroArray);

        // restore current record to what is was before processing this complex
        // array
        currentAvroRecord = avroParentRecord;

        log.debug(
                "End visiting complex array type {} with {} items in avro schema {}",
                ce.getJaxbName(), ce.getCurrentOccurs(), avroArray.getSchema());
    }

    public void visit(ICobolStringBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolStringConverter);
        store(ce);
    }

    public void visit(ICobolArrayStringBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayStringConverter);
        store(ce);
    }

    public void visit(ICobolNationalBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolNationalConverter);
        store(ce);
    }

    public void visit(ICobolArrayNationalBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayNationalConverter);
        store(ce);
    }

    public void visit(ICobolDbcsBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolDbcsConverter);
        store(ce);
    }

    public void visit(ICobolArrayDbcsBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayDbcsConverter);
        store(ce);
    }

    public void visit(ICobolZonedDecimalBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolZonedDecimalConverter);
        store(ce);
    }

    public void visit(ICobolArrayZonedDecimalBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayZonedDecimalConverter);
        store(ce);
    }

    public void visit(ICobolPackedDecimalBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolPackedDecimalConverter);
        store(ce);
    }

    public void visit(ICobolArrayPackedDecimalBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayPackedDecimalConverter);
        store(ce);
    }

    public void visit(ICobolBinaryBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolBinaryConverter);
        store(ce);
    }

    public void visit(ICobolArrayBinaryBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayBinaryConverter);
        store(ce);
    }

    public void visit(ICobolFloatBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolFloatConverter);
        store(ce);
    }

    public void visit(ICobolArrayFloatBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayFloatConverter);
        store(ce);
    }

    public void visit(ICobolDoubleBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolDoubleConverter);
        store(ce);
    }

    public void visit(ICobolArrayDoubleBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayDoubleConverter);
        store(ce);
    }

    public void visit(ICobolOctetStreamBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolOctetStreamConverter);
        store(ce);
    }

    public void visit(ICobolArrayOctetStreamBinding ce) throws HostException {
        visitPrimitiveType(ce, cobolArrayOctetStreamConverter);
        store(ce);
    }

    private <T extends ICobolBinding> void visitPrimitiveType(T ce,
            FromHostConverter < T > converter) throws HostException {
        if (!exists(ce)) {
            return;
        }
        setOffset(converter.fromHost(ce, getHostBytes(), getStartOffset()));

        if (ce.isCustomVariable()) {
            storeCustomVariable(ce);
        }
    }

    /*
     * ------------------------------------------------------------------------
     * Host to java converters for primitive types and primitive type arrays
     * ------------------------------------------------------------------------
     */
    private interface FromHostConverter<T extends ICobolBinding> {
        int fromHost(T ce, byte[] hostSource, int offset) throws HostException;
    }

    private class CobolZonedDecimalConverter implements
            FromHostConverter < ICobolZonedDecimalBinding > {

        public int fromHost(ICobolZonedDecimalBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolZonedDecimalConverter()
                    .fromHost(ce, hostSource, offset);
        }

    }

    private class CobolPackedDecimalConverter implements
            FromHostConverter < ICobolPackedDecimalBinding > {

        public int fromHost(ICobolPackedDecimalBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolPackedDecimalConverter()
                    .fromHost(ce, hostSource, offset);
        }

    }

    private class CobolBinaryConverter implements
            FromHostConverter < ICobolBinaryBinding > {

        public int fromHost(ICobolBinaryBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolBinaryConverter().fromHost(ce,
                    hostSource, offset);
        }

    }

    private class CobolStringConverter implements
            FromHostConverter < ICobolStringBinding > {

        public int fromHost(ICobolStringBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolStringConverter().fromHost(ce,
                    hostSource, offset);
        }

    }

    private class CobolNationalConverter implements
            FromHostConverter < ICobolNationalBinding > {

        public int fromHost(ICobolNationalBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolNationalConverter().fromHost(
                    ce, hostSource, offset);
        }

    }

    private class CobolDbcsConverter implements
            FromHostConverter < ICobolDbcsBinding > {

        public int fromHost(ICobolDbcsBinding ce, byte[] hostSource, int offset)
                throws HostException {
            return getCobolConverters().getCobolDbcsConverter().fromHost(ce,
                    hostSource, offset);
        }

    }

    private class CobolFloatConverter implements
            FromHostConverter < ICobolFloatBinding > {

        public int fromHost(ICobolFloatBinding ce, byte[] hostSource, int offset)
                throws HostException {
            return getCobolConverters().getCobolFloatConverter().fromHost(ce,
                    hostSource, offset);
        }

    }

    private class CobolDoubleConverter implements
            FromHostConverter < ICobolDoubleBinding > {

        public int fromHost(ICobolDoubleBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolDoubleConverter().fromHost(ce,
                    hostSource, offset);
        }

    }

    private class CobolOctetStreamConverter implements
            FromHostConverter < ICobolOctetStreamBinding > {

        public int fromHost(ICobolOctetStreamBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolOctetStreamConverter()
                    .fromHost(ce, hostSource, offset);
        }

    }

    private class CobolArrayZonedDecimalConverter implements
            FromHostConverter < ICobolArrayZonedDecimalBinding > {

        public int fromHost(ICobolArrayZonedDecimalBinding ce,
                byte[] hostSource, int offset) throws HostException {
            return getCobolConverters().getCobolZonedDecimalConverter()
                    .fromHost(ce, hostSource, offset, ce.getCurrentOccurs());
        }

    }

    private class CobolArrayPackedDecimalConverter implements
            FromHostConverter < ICobolArrayPackedDecimalBinding > {

        public int fromHost(ICobolArrayPackedDecimalBinding ce,
                byte[] hostSource, int offset) throws HostException {
            return getCobolConverters().getCobolPackedDecimalConverter()
                    .fromHost(ce, hostSource, offset, ce.getCurrentOccurs());
        }

    }

    private class CobolArrayBinaryConverter implements
            FromHostConverter < ICobolArrayBinaryBinding > {

        public int fromHost(ICobolArrayBinaryBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolBinaryConverter().fromHost(ce,
                    hostSource, offset, ce.getCurrentOccurs());
        }

    }

    private class CobolArrayStringConverter implements
            FromHostConverter < ICobolArrayStringBinding > {

        public int fromHost(ICobolArrayStringBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolStringConverter().fromHost(ce,
                    hostSource, offset, ce.getCurrentOccurs());
        }

    }

    private class CobolArrayNationalConverter implements
            FromHostConverter < ICobolArrayNationalBinding > {

        public int fromHost(ICobolArrayNationalBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolNationalConverter().fromHost(
                    ce, hostSource, offset, ce.getCurrentOccurs());
        }

    }

    private class CobolArrayDbcsConverter implements
            FromHostConverter < ICobolArrayDbcsBinding > {

        public int fromHost(ICobolArrayDbcsBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolDbcsConverter().fromHost(ce,
                    hostSource, offset, ce.getCurrentOccurs());
        }

    }

    private class CobolArrayFloatConverter implements
            FromHostConverter < ICobolArrayFloatBinding > {

        public int fromHost(ICobolArrayFloatBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolFloatConverter().fromHost(ce,
                    hostSource, offset, ce.getCurrentOccurs());
        }

    }

    private class CobolArrayDoubleConverter implements
            FromHostConverter < ICobolArrayDoubleBinding > {

        public int fromHost(ICobolArrayDoubleBinding ce, byte[] hostSource,
                int offset) throws HostException {
            return getCobolConverters().getCobolDoubleConverter().fromHost(ce,
                    hostSource, offset, ce.getCurrentOccurs());
        }

    }

    private class CobolArrayOctetStreamConverter implements
            FromHostConverter < ICobolArrayOctetStreamBinding > {

        public int fromHost(ICobolArrayOctetStreamBinding ce,
                byte[] hostSource, int offset) throws HostException {
            return getCobolConverters().getCobolOctetStreamConverter()
                    .fromHost(ce, hostSource, offset, ce.getCurrentOccurs());
        }

    }

    /*
     * ------------------------------------------------------------------------
     * Store unmarshalled values in current avro record
     * ------------------------------------------------------------------------
     */
    private void store(ICobolStringBinding ce) throws HostException {
        storeString(ce, ce.getStringValue());
    }

    private void store(ICobolNationalBinding ce) throws HostException {
        storeString(ce, ce.getStringValue());
    }

    private void store(ICobolDbcsBinding ce) throws HostException {
        storeString(ce, ce.getStringValue());
    }

    private void storeString(ICobolBinding ce, String value)
            throws HostException {
        String fieldName = getAvroFieldName(ce);
        putValue(fieldName, value);
    }

    private void store(ICobolArrayStringBinding ce) throws HostException {
        storeArray(ce, ce.getStringList());
    }

    private void store(ICobolArrayNationalBinding ce) throws HostException {
        storeArray(ce, ce.getStringList());
    }

    private void store(ICobolArrayDbcsBinding ce) throws HostException {
        storeArray(ce, ce.getStringList());
    }

    private void store(ICobolArrayFloatBinding ce) throws HostException {
        storeArray(ce, ce.getFloatList());
    }

    private void store(ICobolArrayDoubleBinding ce) throws HostException {
        storeArray(ce, ce.getDoubleList());
    }

    private void store(ICobolArrayOctetStreamBinding ce) throws HostException {
        storeArray(ce, ce.getByteArrayList());
    }

    private <T> void storeArray(ICobolArrayBinding ce, List < T > items)
            throws HostException {
        String fieldName = getAvroFieldName(ce);
        Schema schema = getSchema(fieldName);
        GenericData.Array < T > avroStringArray = new GenericData.Array < T >(
                ce.getCurrentOccurs(), schema);
        avroStringArray.addAll(items);
        putValue(fieldName, avroStringArray);
    }

    private void store(ICobolNumericBinding ce) throws HostException {
        String fieldName = getAvroFieldName(ce);
        putValue(fieldName,
                getValue(ce, getSchema(currentAvroRecord, fieldName)));
    }

    private void store(ICobolFloatBinding ce) throws HostException {
        String fieldName = getAvroFieldName(ce);
        putValue(fieldName, ce.getFloatValue());
    }

    private void store(ICobolDoubleBinding ce) throws HostException {
        String fieldName = getAvroFieldName(ce);
        putValue(fieldName, ce.getDoubleValue());
    }

    private void store(ICobolOctetStreamBinding ce) throws HostException {
        String fieldName = getAvroFieldName(ce);
        Schema schema = getSchema(fieldName);
        GenericData.Fixed genericFixed = new GenericData.Fixed(schema, ce.getByteArrayValue());
        putValue(fieldName, genericFixed);
    }

    /**
     * Retrieve the avro schema for a field in the current record.
     * <p/>
     * If the field is not in the parent record, assume it is the current choice alternative schema.
     * 
     * @param fieldName the field name
     * @return the avri field schema (type)
     * @throws HostException if field does not have a schema
     */
    private Schema getSchema(String fieldName)
            throws HostException {
        return getSchema(currentAvroRecord, fieldName);
    }

    /**
     * Retrieve the avro schema for a field.
     * <p/>
     * If the field is not in the parent record, assume it is the current choice alternative schema.
     * 
     * @param parentRecord the avro parent record
     * @param fieldName the field name
     * @return the avri field schema (type)
     * @throws HostException if field does not have a schema
     */
    private Schema getSchema(GenericRecord parentRecord, String fieldName)
            throws HostException {
        Schema schema = null;
        Field field = parentRecord.getSchema().getField(fieldName);
        if (field == null) {
            if (currentAvroChoiceAlternativeSchema != null) {
                schema = currentAvroChoiceAlternativeSchema;
            }
        } else {
            schema = field.schema();
        }
        if (schema == null) {
            throw new HostException("Field " + fieldName
                    + " is not a field in " + parentRecord.getSchema());
        }
        return schema;
    }

    private void store(ICobolArrayNumericBinding ce) throws HostException {
        String fieldName = getAvroFieldName(ce);
        Schema schema = getSchema(fieldName);
        switch (schema.getElementType().getType()) {
        case INT:
            GenericData.Array < Integer > avroIntArray = new GenericData.Array < Integer >(
                    ce.getCurrentOccurs(), schema);
            avroIntArray.addAll(ce.getIntegerList());
            putValue(fieldName, avroIntArray);
            break;
        case LONG:
            GenericData.Array < Long > avroLongArray = new GenericData.Array < Long >(
                    ce.getCurrentOccurs(), schema);
            avroLongArray.addAll(ce.getLongList());
            putValue(fieldName, avroLongArray);
            break;
        case BYTES:
            GenericData.Array < ByteBuffer > avroBytesArray = new GenericData.Array < ByteBuffer >(
                    ce.getCurrentOccurs(), schema);
            for (BigDecimal value : ce.getBigDecimalList()) {
                avroBytesArray.add(DecimalUtils.toByteBuffer(value));
            }
            putValue(fieldName, avroBytesArray);
            break;
        default:
            throw new HostException("Schema type " + schema.getType()
                    + " does not map to a numeric");
        }
    }

    private Object getValue(ICobolNumericBinding ce, Schema schema)
            throws HostException {
        switch (schema.getType()) {
        case INT:
            return ce.getIntegerValue();
        case LONG:
            return ce.getLongValue();
        case BYTES:
            return DecimalUtils.toByteBuffer(ce.getBigDecimalValue());
        default:
            throw new HostException("Schema type " + schema.getType()
                    + " does not map to a numeric");
        }
    }
    
    /**
     * Populates the current avro record with a value.
     * <p/>
     * If the field name is not in the current record schema it might be because
     * we are populating a choice rather than the record itself.
     * 
     * @param fieldName the field to set
     * @param value the value to set
     */
    private void putValue(String fieldName, Object value) {
        putValue(currentAvroRecord, fieldName, value);
    }

    /**
     * Populates an avro record with a value.
     * <p/>
     * If the field name is not in the avro record schema it might be because
     * we are populating a choice rather than the record itself.
     * 
     * @param record the field to set
     * @param fieldName the field to set
     * @param value the value to set
     */
    private void putValue(GenericRecord record, String fieldName, Object value) {
        Field field = record.getSchema().getField(fieldName);
        if (field == null) {
            if (currentAvroChoiceFieldName != null) {
                record.put(currentAvroChoiceFieldName, value);
            }
        } else {
            record.put(fieldName, value);
        }
    }
    
    /*
     * ------------------------------------------------------------------------
     * Utils
     * ------------------------------------------------------------------------
     */
    private <T extends ICobolBinding> String getAvroFieldName(T ce) {
        return NameUtil.toVariableName(ce.getJaxbName());

    }

    private <T extends ICobolBinding> String getAvroTypeName(T ce) {
        return NameUtil.toClassName(ce.getJaxbName());

    }

    /*
     * ------------------------------------------------------------------------
     * Getters/Setters
     * ------------------------------------------------------------------------
     */
    public GenericRecord getRootAvroRecord() {
        return rootAvroRecord;
    }

}
