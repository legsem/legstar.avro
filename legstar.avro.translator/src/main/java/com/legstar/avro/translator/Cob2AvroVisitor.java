package com.legstar.avro.translator;

import java.util.Iterator;

import javax.xml.namespace.QName;

import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaComplexType;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaFacet;
import org.apache.ws.commons.schema.XmlSchemaFractionDigitsFacet;
import org.apache.ws.commons.schema.XmlSchemaGroup;
import org.apache.ws.commons.schema.XmlSchemaGroupRef;
import org.apache.ws.commons.schema.XmlSchemaMaxLengthFacet;
import org.apache.ws.commons.schema.XmlSchemaObject;
import org.apache.ws.commons.schema.XmlSchemaObjectCollection;
import org.apache.ws.commons.schema.XmlSchemaObjectTable;
import org.apache.ws.commons.schema.XmlSchemaParticle;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.XmlSchemaSimpleType;
import org.apache.ws.commons.schema.XmlSchemaSimpleTypeRestriction;
import org.apache.ws.commons.schema.XmlSchemaTotalDigitsFacet;
import org.apache.ws.commons.schema.constants.Constants;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.legstar.coxb.util.NameUtil;

/**
 * A special version of an XSD navigator that will process array occurrences.
 * This is needed when each occurrence of an array must produce a different
 * output.
 * <p/>
 * TODO process nullable values (strings, arrays depending on, choices, ...)
 * 
 */
public class Cob2AvroVisitor {

    /** Logging. */
    private static Logger log = LoggerFactory.getLogger(Cob2AvroVisitor.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Used to tag columns which belong to choices (REDEFINES). */
    public static final String CHOICE_PREFIX_FORMAT = "[choice:%s_%s]";

    /** The XML schema with COBOL annotations. */
    private final XmlSchema xmlSchema;

    /** Avro JSON schema produced */
    private ObjectNode rootAvroSchema;

    /** Name of the avro schema (case where there are multiple roots in the xsd) */
    private final String avroSchemaName;

    /** namespace for avro schema (package name for generated objects) */
    private final String avroNamespacePrefix;

    public Cob2AvroVisitor(XmlSchema xmlSchema, String avroSchemaName,
            String avroNamespacePrefix) {
        this.xmlSchema = xmlSchema;
        this.avroSchemaName = avroSchemaName;
        this.avroNamespacePrefix = avroNamespacePrefix;
    }

    /**
     * Process each element in the input Schema.
     */
    public void visit() throws Cob2AvroException {

        log.debug("visit XML Schema started");

        final XmlSchemaObjectTable items = xmlSchema.getElements();

        if (items.getCount() > 1) {
            // More then one root, create an aggregate record
            rootAvroSchema = getAvroRecordType(avroSchemaName,
                    xmlSchema.getItems(), 1, false);

        } else if (items.getCount() == 1) {
            XmlSchemaElement xsdElement = (XmlSchemaElement) items.getValues()
                    .next();
            if (xsdElement.getSchemaType() instanceof XmlSchemaComplexType) {
                rootAvroSchema = getAvroRecordType(getAvroTypeName(xsdElement),
                        (XmlSchemaComplexType) xsdElement.getSchemaType(), 1,
                        false);
            } else {
                throw new Cob2AvroException(
                        "XML schema does contains a root element but it is not a complex type");
            }

        } else {
            throw new Cob2AvroException(
                    "XML schema does not contain a root element");
        }
        rootAvroSchema.put("namespace", (avroNamespacePrefix == null ? ""
                : (avroNamespacePrefix + ".")) + avroSchemaName.toLowerCase());

        log.debug("visit XML Schema ended");
    }

    /**
     * Take all elements from a collection and process them.
     * 
     * @param items the parent collection
     * @param level the current level in the elements hierarchy.
     * @param avroFields array of fields being populated
     * @throws Cob2AvroException if processing fails
     */
    protected void processCollectionElements(
            final XmlSchemaObjectCollection items, final int level,
            final ArrayNode avroFields) throws Cob2AvroException {

        if (items == null) {
            return;
        }
        int nextLevel = level + 1;

        /* Process each element in the collection */
        for (int i = 0; i < items.getCount(); i++) {
            XmlSchemaObject element = items.getItem(i);
            if (element instanceof XmlSchemaElement) {
                /* A regular element */
                processElement((XmlSchemaElement) element, nextLevel,
                        avroFields);
            } else if (element instanceof XmlSchemaGroupRef) {
                /* This is a reference to a group so we fetch the group */
                XmlSchemaGroupRef groupRef = (XmlSchemaGroupRef) element;
                XmlSchemaGroup group = (XmlSchemaGroup) xmlSchema.getGroups()
                        .getItem(groupRef.getRefName());
                processCollectionElements(
                        getXsdFields(group.getName(), group.getParticle()),
                        nextLevel, avroFields);
            } else if (element instanceof XmlSchemaChoice) {
                /* A choice element */
                processChoiceElement((XmlSchemaChoice) element, nextLevel,
                        avroFields);
            }

        }
    }

    /**
     * Process a choice element.
     * 
     * @param choice the choice element
     * @param level the current level in the elements hierarchy.
     * @param avroFields array of fields being populated
     * @throws Cob2AvroException
     */
    private void processChoiceElement(XmlSchemaChoice choice, final int level,
            final ArrayNode avroFields) throws Cob2AvroException {

        log.debug("process started for choice");

        XmlSchemaObjectCollection alternatives = choice.getItems();
        String firstAlternativeName = null;
        ArrayNode alternativeAvroTypes = MAPPER.createArrayNode();

        for (int j = 0; j < alternatives.getCount(); j++) {
            XmlSchemaObject alternative = alternatives.getItem(j);
            if (alternative instanceof XmlSchemaElement) {
                String alternativeName = getAvroFieldName((XmlSchemaElement) alternative);
                if (firstAlternativeName == null) {
                    firstAlternativeName = alternativeName;
                }
                processType((XmlSchemaElement) alternative, level,
                        alternativeAvroTypes);
            }
        }
        ObjectNode avroChoiceField = MAPPER.createObjectNode();
        avroChoiceField.put("name", firstAlternativeName + "Choice");
        avroChoiceField.put("type", alternativeAvroTypes);

        avroFields.add(avroChoiceField);
        log.debug("process ended for choice = " + firstAlternativeName
                + "Choice");
    }

    /**
     * Process a regular XML schema element.
     * 
     * @param xsdElement the XML Schema element to process
     * @param level the current level in the elements hierarchy.
     * @param avroFields array of fields being populated
     * @throws Cob2AvroException if processing fails
     */
    private void processElement(final XmlSchemaElement xsdElement,
            final int level, final ArrayNode avroFields)
            throws Cob2AvroException {
        /*
         * If this element is referencing another, it might not be useful to
         * process it.
         */
        if (xsdElement.getRefName() != null) {
            return;
        }
        log.debug("process started for element = " + xsdElement.getName());

        if (xsdElement.getSchemaType() instanceof XmlSchemaComplexType) {

            ObjectNode avroRecordType = getAvroRecordType(
                    getAvroTypeName(xsdElement),
                    (XmlSchemaComplexType) xsdElement.getSchemaType(), level,
                    xsdElement.getMaxOccurs() > 1);
            ObjectNode avroRecordElement = MAPPER.createObjectNode();
            avroRecordElement.put("type", avroRecordType);
            avroRecordElement.put("name", getAvroFieldName(xsdElement));
            avroFields.add(avroRecordElement);

        } else if (xsdElement.getSchemaType() instanceof XmlSchemaSimpleType) {
            addSimpleTypeElement(xsdElement, level,
                    getAvroFieldName(xsdElement), avroFields);
        }

        log.debug("process ended for element = " + xsdElement.getName());
    }

    /**
     * Add an avro type to an array of avro types
     * 
     * @param xsdElement the XML schema element
     * @param level the current level in the elements hierarchy.
     * @param avroTypes array of types being populated
     * @throws Cob2AvroException
     */
    private void processType(final XmlSchemaElement xsdElement,
            final int level, final ArrayNode avroTypes)
            throws Cob2AvroException {

        log.debug("process started for type = " + xsdElement.getName());

        ObjectNode avroType = null;
        if (xsdElement.getSchemaType() instanceof XmlSchemaComplexType) {
            avroType = getAvroRecordType(getAvroTypeName(xsdElement),
                    (XmlSchemaComplexType) xsdElement.getSchemaType(), level,
                    xsdElement.getMaxOccurs() > 1);
        } else if (xsdElement.getSchemaType() instanceof XmlSchemaSimpleType) {
            Object avroPrimitiveType = getAvroPrimitiveType(
                    getAvroTypeName(xsdElement),
                    (XmlSchemaSimpleType) xsdElement.getSchemaType(),
                    xsdElement.getMaxOccurs() > 1);
            avroType = MAPPER.createObjectNode();
            avroType.put("name", getAvroTypeName(xsdElement));
            if (avroPrimitiveType != null) {
                if (avroPrimitiveType instanceof String) {
                    avroType.put("type", (String) avroPrimitiveType);
                } else if (avroPrimitiveType instanceof JsonNode) {
                    avroType.put("type", (JsonNode) avroPrimitiveType);
                }
            }

        }
        avroTypes.add(avroType);
        log.debug("process ended for type = " + xsdElement.getName());

    }

    /**
     * Given an XML schema particle, retrieve a collection or children elements.
     * 
     * @param parentName the parent name (for error reporting)
     * @param particle the XML schema particle
     * @return a collection of children elements or null if none is found
     */
    private XmlSchemaObjectCollection getXsdFields(final QName parentName,
            final XmlSchemaParticle particle) {

        if (particle == null) {
            log.warn("Schema object " + parentName
                    + " does not contain a particle");
            return null;
        }

        if (particle.getMaxOccurs() > 1) {
            /* TODO find a way to handle occuring sequences and alls */
            log.warn("Schema object " + parentName
                    + " contains a multi-occurence particle that is ignored");
        }

        if (particle instanceof XmlSchemaSequence) {
            XmlSchemaSequence sequence = (XmlSchemaSequence) particle;
            return sequence.getItems();

        } else if (particle instanceof XmlSchemaAll) {
            XmlSchemaAll all = (XmlSchemaAll) particle;
            return all.getItems();
        } else {
            /* TODO process other particle types of interest */
            /* TODO find a way to handle xsd:attribute */
            log.warn("Schema object " + parentName
                    + " does not contain a sequence or all element");
            return null;
        }
    }

    /**
     * Create an avro primitive type field using the XML schema and COBOL
     * annotations.
     * 
     * @param xsdElement the XML schema element
     * @param elc the COBOL annotations as a set of DOM attributes
     * @param level the depth in the hierarchy
     * @param avroFieldName to use as the field name for this element
     * @param avroFields to array of fields being populated
     * @throws Cob2AvroException if something abnormal in the xsd
     */
    private void addSimpleTypeElement(final XmlSchemaElement xsdElement,
            final int level, final String avroFieldName, ArrayNode avroFields)
            throws Cob2AvroException {

        XmlSchemaSimpleType xsdType = (XmlSchemaSimpleType) xsdElement
                .getSchemaType();
        Object avroType = getAvroPrimitiveType(avroFieldName, xsdType,
                xsdElement.getMaxOccurs() > 1);

        ObjectNode avroField = MAPPER.createObjectNode();
        avroField.put("name", avroFieldName);
        if (avroType != null) {
            if (avroType instanceof String) {
                avroField.put("type", (String) avroType);
            } else if (avroType instanceof JsonNode) {
                avroField.put("type", (JsonNode) avroType);
            }
        }

        log.debug("Produced: {}", avroField.toString());
        avroFields.add(avroField);
    }

    /**
     * For complex types, builds an avro Record type.
     * 
     * @param avroRecordTypeName the avro record type name
     * @param xsdComplexType the XML schema complex type
     * @param level the current level in the hierarchy
     * @param isArray whether this type is an array (a complex array then)
     * @return the avro record type
     * @throws Cob2AvroException
     */
    private ObjectNode getAvroRecordType(String avroRecordTypeName,
            XmlSchemaComplexType xsdComplexType, final int level,
            boolean isArray) throws Cob2AvroException {

        return getAvroRecordType(
                avroRecordTypeName,
                getXsdFields(xsdComplexType.getQName(),
                        xsdComplexType.getParticle()), level, isArray);
    }

    /**
     * For complex types, builds an avro Record type.
     * 
     * @param avroRecordTypeName the avro record type name
     * @param xsdFields the XML schema fields for the complex type
     * @param level the current level in the hierarchy
     * @param isArray whether this type is an array (a complex array then)
     * @return the avro record type
     * @throws Cob2AvroException
     */
    private ObjectNode getAvroRecordType(String avroRecordTypeName,
            XmlSchemaObjectCollection xsdFields, final int level,
            boolean isArray) throws Cob2AvroException {

        ObjectNode avroRecord = MAPPER.createObjectNode();
        avroRecord.put("type", "record");
        avroRecord.put("name", avroRecordTypeName);
        final ArrayNode fields = MAPPER.createArrayNode();

        processCollectionElements(xsdFields, level, fields);
        avroRecord.put("fields", fields);

        if (isArray) {
            ObjectNode avroArray = MAPPER.createObjectNode();
            avroArray.put("type", "array");
            avroArray.put("items", avroRecord);
            return avroArray;
        }
        return avroRecord;
    }

    /**
     * Get an Avro Type derived from the XSD Type for the field being built.
     * <p/>
     * The type returned might be a complex one in the case of fixed, bytes or
     * arrays.
     * 
     * @see <a href="http://avro.apache.org/docs/current/spec.html">avro
     *      specs</a>
     * 
     * @param avroFieldName the json avro field name being built
     * @param xsdType an XML schema Type or null if none can be derived
     * @param isArray if this is an array of simple types
     * @return a String or JSON object representing the schema of the simple
     *         type
     * @throws Cob2AvroException if something abnormal in the xsd
     */
    @SuppressWarnings("unchecked")
    private Object getAvroPrimitiveType(final String avroFieldName,
            final XmlSchemaSimpleType xsdType, boolean isArray)
            throws Cob2AvroException {
        XmlSchemaSimpleTypeRestriction restriction = (XmlSchemaSimpleTypeRestriction) xsdType
                .getContent();
        if (restriction != null && restriction.getBaseTypeName() != null) {
            QName xsdTypeName = restriction.getBaseTypeName();
            ObjectNode avroType = null; // Null for primitive types
            String avroPrimitiveType = getAvroPrimitiveType(xsdTypeName);
            if (avroPrimitiveType == null) {
                return null;
            }
            if ("fixed".equals(avroPrimitiveType)) {
                if (restriction.getFacets() == null) {
                    throw new Cob2AvroException(
                            "Binary type without facets in " + avroFieldName);
                }
                avroType = MAPPER.createObjectNode();
                avroType.put("type", "fixed");
                avroType.put("name", NameUtil.toClassName(avroFieldName));
                for (Iterator < XmlSchemaObject > i = restriction.getFacets()
                        .getIterator(); i.hasNext();) {
                    XmlSchemaObject facet = i.next();
                    if (facet instanceof XmlSchemaMaxLengthFacet) {
                        XmlSchemaMaxLengthFacet xsef = (XmlSchemaMaxLengthFacet) facet;
                        avroType.put("size",
                                Integer.parseInt((String) xsef.getValue()));
                    }
                }
            } else if ("decimal".equals(avroPrimitiveType)) {
                if (restriction.getFacets() == null) {
                    throw new Cob2AvroException(
                            "Decimal type without facets in " + avroFieldName);
                }
                avroType = MAPPER.createObjectNode();
                avroType.put("type", "bytes");
                avroType.put("logicalType", "decimal");
                for (Iterator < XmlSchemaFacet > i = restriction.getFacets()
                        .getIterator(); i.hasNext();) {
                    XmlSchemaFacet facet = i.next();
                    if (facet instanceof XmlSchemaTotalDigitsFacet) {
                        avroType.put("precision",
                                Integer.parseInt((String) facet.getValue()));
                    } else if (facet instanceof XmlSchemaFractionDigitsFacet) {
                        avroType.put("scale",
                                Integer.parseInt((String) facet.getValue()));
                    }
                }
            }

            if (isArray) {
                ObjectNode avroArrayType = MAPPER.createObjectNode();
                avroArrayType.put("type", "array");
                if (avroType == null) {
                    avroArrayType.put("items", avroPrimitiveType);
                } else {
                    avroArrayType.put("items", avroType);
                }
                return avroArrayType;

            } else {
                if (avroType == null) {
                    return avroPrimitiveType;
                } else {
                    return avroType;
                }
            }
        } else {
            log.warn("Simple type without restriction " + xsdType.getName());
            return null;
        }

    }

    private String getAvroPrimitiveType(QName xsdTypeName) {
        if (xsdTypeName.equals(Constants.XSD_STRING)) {
            return "string";
        } else if (xsdTypeName.equals(Constants.XSD_INT)) {
            return "int";
        } else if (xsdTypeName.equals(Constants.XSD_INTEGER)) {
            return "long";
        } else if (xsdTypeName.equals(Constants.XSD_LONG)) {
            return "long";
        } else if (xsdTypeName.equals(Constants.XSD_SHORT)) {
            return "int";
        } else if (xsdTypeName.equals(Constants.XSD_DECIMAL)) {
            // Avro stores decimals unscaled as 2's complement
            return "decimal"; // Not a real avro type
        } else if (xsdTypeName.equals(Constants.XSD_FLOAT)) {
            return "float";
        } else if (xsdTypeName.equals(Constants.XSD_DOUBLE)) {
            return "double";
        } else if (xsdTypeName.equals(Constants.XSD_HEXBIN)) {
            // Avro stores these as fixed but requires a size
            return "fixed";
        } else if (xsdTypeName.equals(Constants.XSD_UNSIGNEDINT)) {
            return "long";
        } else if (xsdTypeName.equals(Constants.XSD_UNSIGNEDSHORT)) {
            return "int";
        } else if (xsdTypeName.equals(Constants.XSD_UNSIGNEDLONG)) {
            return "long";
        } else {
            log.warn("Unable to derive Avro type from "
                    + xsdTypeName.getLocalPart());
            return null;
        }
    }

    /**
     * Get a name for a field.
     * <p/>
     * It is important that these names map corresponding legstar names (hence
     * use of the {@link NameUtil} class.
     * 
     * @param xsdElement the COBOL-annotated XML schema element name.
     * @return a field name
     */
    public String getAvroFieldName(final XmlSchemaElement xsdElement) {
        return NameUtil.toVariableName(xsdElement.getName());
    }

    /**
     * Get a name for a record type.
     * <p/>
     * It is important that these names map corresponding legstar names (hence
     * use of the {@link NameUtil} class.
     * 
     * @param xsdElement the COBOL-annotated XML schema element name.
     * @return a field name
     */
    public String getAvroTypeName(final XmlSchemaElement xsdElement) {
        return NameUtil.toClassName(xsdElement.getName());
    }

    /**
     * @return XML schema with COBOL annotations
     */
    public XmlSchema getSchema() {
        return xmlSchema;
    }

    /**
     * @return the cascading Fields produced
     */
    public ObjectNode getAvroSchema() {
        return rootAvroSchema;
    }

    /**
     * @return the cascading Fields produced
     */
    public String getAvroSchemaAsString() {
        return getAvroSchema().toString();
    }

}
