package com.legstar.avro.translator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaAll;
import org.apache.ws.commons.schema.XmlSchemaAllMember;
import org.apache.ws.commons.schema.XmlSchemaChoice;
import org.apache.ws.commons.schema.XmlSchemaChoiceMember;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.apache.ws.commons.schema.XmlSchemaComplexType;
import org.apache.ws.commons.schema.XmlSchemaElement;
import org.apache.ws.commons.schema.XmlSchemaFacet;
import org.apache.ws.commons.schema.XmlSchemaFractionDigitsFacet;
import org.apache.ws.commons.schema.XmlSchemaGroup;
import org.apache.ws.commons.schema.XmlSchemaGroupRef;
import org.apache.ws.commons.schema.XmlSchemaMaxLengthFacet;
import org.apache.ws.commons.schema.XmlSchemaObject;
import org.apache.ws.commons.schema.XmlSchemaParticle;
import org.apache.ws.commons.schema.XmlSchemaSequence;
import org.apache.ws.commons.schema.XmlSchemaSequenceMember;
import org.apache.ws.commons.schema.XmlSchemaSimpleType;
import org.apache.ws.commons.schema.XmlSchemaSimpleTypeRestriction;
import org.apache.ws.commons.schema.XmlSchemaTotalDigitsFacet;
import org.apache.ws.commons.schema.constants.Constants;
import org.apache.ws.commons.schema.utils.XmlSchemaObjectBase;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates a COBOL-annotated XML schema to an Avro Schema.
 * 
 */
public class Xsd2AvroTranslator {

    private static final String LEGSTAR_XSD_FILE_ENCODING = "UTF-8";

    /** Logging. */
    private static Logger log = LoggerFactory
            .getLogger(Xsd2AvroTranslator.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    /** Used to tag columns which belong to choices (REDEFINES). */
    public static final String CHOICE_PREFIX_FORMAT = "[choice:%s_%s]";

    /**
     * Translate a legstar generated xsd file (with COBOL annotations) to an
     * avro schema.
     * <p/>
     * If a folder is passed rather than a single file then all xsd files from
     * that folder are translated.
     * 
     * @param xsdInput a single xsd file or a folder containing xsd files
     * @param avroNamespacePrefix for avro schema (package name for generated
     *            objects)
     * @return for each xsd file translated, the avro schema serialized as
     *         string
     * @throws IOException if translation fails
     */
    public Map < String, String > translate(File xsdInput,
            String avroNamespacePrefix) throws IOException {

        log.info("COBOL to Avro translator started");
        Map < String, String > mapSchemas = new HashMap < String, String >();

        if (!xsdInput.exists()) {
            throw new IOException("Specified input '" + xsdInput.getName()
                    + "' does not exist");
        }

        log.info("Reading legstar xsd files from " + xsdInput.getName());

        if (xsdInput.isDirectory()) {
            Collection < File > xsdFiles = FileUtils.listFiles(xsdInput,
                    new String[] { "xsd" }, true);
            for (File xsdFile : xsdFiles) {
                translate(xsdFile, mapSchemas, avroNamespacePrefix);
            }
        } else {
            translate(xsdInput, mapSchemas, avroNamespacePrefix);
        }

        log.info("COBOL to Avro translator ended");
        return mapSchemas;
    }

    private void translate(File xsdFile, Map < String, String > mapSchemas,
            String avroNamespacePrefix) throws IOException {

        String xsdFileName = FilenameUtils.getBaseName(xsdFile.getName());
        String avroNamespace = (avroNamespacePrefix == null ? ""
                : (avroNamespacePrefix + ".")) + xsdFileName.toLowerCase();

        String avroSchema = translate(new InputStreamReader(
                new FileInputStream(xsdFile), LEGSTAR_XSD_FILE_ENCODING),
                avroNamespace, xsdFileName);
        mapSchemas.put(xsdFileName, avroSchema);

        log.info("LegStar xsd {} translated to schema {}", xsdFileName,
                avroSchema);
    }

    /**
     * Translate the input XML Schema (via Reader).
     * 
     * @param reader the input XML Schema reader
     * @param avroNamespace the target Avro namespace
     * @param avroSchemaName the target Avro schema name
     * @return the Avro schema as a JSON string
     * @throws Xsd2AvroTranslatorException if translation fails
     */
    public String translate(Reader reader, String avroNamespace,
            String avroSchemaName) throws Xsd2AvroTranslatorException {

        XmlSchemaCollection schemaCol = new XmlSchemaCollection();
        XmlSchema xsd = schemaCol.read(new StreamSource(reader));
        return translate(xsd, avroNamespace, avroSchemaName);
    }

    /**
     * Translate the input XML Schema.
     * 
     * @param xmlSchema the input XML Schema
     * @param avroNamespace the target Avro namespace
     * @param avroSchemaName the target Avro schema name
     * @return the Avro schema as a JSON string
     * @throws Xsd2AvroTranslatorException if translation fails
     */
    public String translate(XmlSchema xmlSchema, String avroNamespace,
            String avroSchemaName) throws Xsd2AvroTranslatorException {

        log.debug("XML Schema to Avro Schema translator started");

        final Map < QName, XmlSchemaElement > items = xmlSchema.getElements();
        ObjectNode rootAvroSchema = null;

        final ArrayNode avroFields = MAPPER.createArrayNode();
        if (items.size() > 1) {
            // More then one root, create an aggregate record
            for (Entry < QName, XmlSchemaElement > entry : items.entrySet()) {
                visit(xmlSchema, entry.getValue(), 1, avroFields);
            }
            rootAvroSchema = buildAvroRecordType(avroSchemaName, avroFields,
                    false);

        } else if (items.size() == 1) {
            XmlSchemaElement xsdElement = (XmlSchemaElement) items.values()
                    .iterator().next();
            if (xsdElement.getSchemaType() instanceof XmlSchemaComplexType) {
                XmlSchemaComplexType xsdType = (XmlSchemaComplexType) xsdElement
                        .getSchemaType();
                visit(xmlSchema, xsdType, 1, avroFields);
                rootAvroSchema = buildAvroRecordType(getAvroTypeName(xsdType),
                        avroFields, xsdElement.getMaxOccurs() > 1);
            } else {
                throw new Xsd2AvroTranslatorException(
                        "XML schema does contain a root element but it is not a complex type");
            }

        } else {
            throw new Xsd2AvroTranslatorException(
                    "XML schema does not contain a root element");
        }
        rootAvroSchema.put("namespace", avroNamespace == null ? ""
                : avroNamespace);

        log.debug("XML Schema to Avro Schema translator  ended");

        return rootAvroSchema.toString();
    }

    private ObjectNode buildAvroRecordType(String avroRecordTypeName,
            final ArrayNode avroFields, boolean isArray)
            throws Xsd2AvroTranslatorException {

        ObjectNode avroRecord = MAPPER.createObjectNode();
        avroRecord.put("type", "record");
        avroRecord.put("name", avroRecordTypeName);
        avroRecord.put("fields", avroFields);

        if (isArray) {
            ObjectNode avroArray = MAPPER.createObjectNode();
            avroArray.put("type", "array");
            avroArray.put("items", avroRecord);
            return avroArray;
        }
        return avroRecord;
    }

    /**
     * Process a regular XML schema element.
     * 
     * @param xmlSchema the input XML schema
     * @param xsdElement the XML Schema element to process
     * @param level the current level in the elements hierarchy.
     * @param avroFields array of fields being populated
     * @throws Xsd2AvroTranslatorException if processing fails
     */
    private void visit(XmlSchema xmlSchema, final XmlSchemaElement xsdElement,
            final int level, final ArrayNode avroFields)
            throws Xsd2AvroTranslatorException {

        /*
         * If this element is referencing another, it might not be useful to
         * process it.
         */
        if (xsdElement.getRef().getTarget() != null) {
            return;
        }
        log.debug("process started for element = " + xsdElement.getName());

        if (xsdElement.getSchemaType() instanceof XmlSchemaComplexType) {
            XmlSchemaComplexType xsdType = (XmlSchemaComplexType) xsdElement
                    .getSchemaType();

            int nextLevel = level + 1;
            final ArrayNode avroChildrenFields = MAPPER.createArrayNode();
            visit(xmlSchema, xsdType, nextLevel, avroChildrenFields);
            ObjectNode avroRecordType = buildAvroRecordType(
                    getAvroTypeName(xsdType), avroChildrenFields,
                    xsdElement.getMaxOccurs() > 1);
            ObjectNode avroRecordElement = MAPPER.createObjectNode();
            avroRecordElement.put("type", avroRecordType);
            avroRecordElement.put("name", getAvroFieldName(xsdElement));
            avroFields.add(avroRecordElement);

        } else if (xsdElement.getSchemaType() instanceof XmlSchemaSimpleType) {
            visit((XmlSchemaSimpleType) xsdElement.getSchemaType(), level,
                    getAvroFieldName(xsdElement), xsdElement.getMaxOccurs(),
                    avroFields);
        }

        log.debug("process ended for element = " + xsdElement.getName());
    }

    /**
     * Create an avro complex type field using an XML schema type.
     * 
     * @param xsdType the XML schema complex type
     * @param level the depth in the hierarchy
     * @param avroFields array of avro fields being populated
     * @throws Xsd2AvroTranslatorException if something abnormal in the xsd
     */
    private void visit(XmlSchema xmlSchema, XmlSchemaComplexType xsdType,
            final int level, final ArrayNode avroFields) {

        visit(xmlSchema, xsdType.getParticle(), level, avroFields);

    }

    /**
     * Create an avro primitive type field using an XML schema type.
     * 
     * @param xsdType the XML schema simple type
     * @param level the depth in the hierarchy
     * @param avroFieldName to use as the field name for this avro field
     * @param maxOccurs dimension for arrays
     * @param avroFields array of avro fields being populated
     * @throws Xsd2AvroTranslatorException if something abnormal in the xsd
     */
    private void visit(XmlSchemaSimpleType xsdType, final int level,
            final String avroFieldName, long maxOccurs, ArrayNode avroFields)
            throws Xsd2AvroTranslatorException {

        Object avroType = getAvroPrimitiveType(avroFieldName, xsdType,
                maxOccurs > 1);

        ObjectNode avroField = MAPPER.createObjectNode();
        avroField.put("name", avroFieldName);
        if (avroType != null) {
            if (avroType instanceof String) {
                avroField.put("type", (String) avroType);
            } else if (avroType instanceof JsonNode) {
                avroField.put("type", (JsonNode) avroType);
            }
        }

        avroFields.add(avroField);
    }

    private void visit(XmlSchema xmlSchema, XmlSchemaParticle particle,
            final int level, final ArrayNode avroFields) {

        if (particle.getMaxOccurs() > 1) {
            /* TODO find a way to handle occuring sequences and alls */
            log.warn("Schema object at line " + particle.getLineNumber()
                    + " contains a multi-occurence particle that is ignored");
        }

        if (particle instanceof XmlSchemaSequence) {
            XmlSchemaSequence sequence = (XmlSchemaSequence) particle;
            for (XmlSchemaSequenceMember member : sequence.getItems()) {
                visit(xmlSchema, member, level, avroFields);
            }

        } else if (particle instanceof XmlSchemaAll) {
            XmlSchemaAll all = (XmlSchemaAll) particle;
            for (XmlSchemaAllMember member : all.getItems()) {
                visit(xmlSchema, member, level, avroFields);
            }

        } else {
            /* TODO process other particle types of interest */
            /* TODO find a way to handle xsd:attribute */
            log.warn("Schema object does not contain a sequence or all element at line "
                    + particle.getLineNumber());
        }
    }

    /**
     * Visit a base XSD element.
     * <p/>
     * Could be any of:
     * <ul>
     * <li>A regular element</li>
     * <li>A reference to a group</li>
     * <li>A choice</li>
     * </ul>
     * 
     * @param xmlSchema the input XML schema
     * @param element the base element
     * @param level the current level in the hierarchy
     * @param avroFields an array of avro fields being populated
     */
    private void visit(XmlSchema xmlSchema, XmlSchemaObjectBase element,
            final int level, final ArrayNode avroFields) {
        if (element instanceof XmlSchemaElement) {
            visit(xmlSchema, (XmlSchemaElement) element, level, avroFields);
        } else if (element instanceof XmlSchemaGroupRef) {
            XmlSchemaGroupRef groupRef = (XmlSchemaGroupRef) element;
            XmlSchemaGroup group = xmlSchema.getGroups().get(
                    groupRef.getRefName());
            visit(xmlSchema, group.getParticle(), level, avroFields);
        } else if (element instanceof XmlSchemaChoice) {
            visit(xmlSchema, (XmlSchemaChoice) element, level, avroFields);
        }
    }

    /**
     * Process a choice element.
     * <p/>
     * Visits all elements from the choice then creates an avro Choice using the
     * types of each alternative.
     * 
     * @param xmlSchema the input XML schema
     * @param xsdChoice the choice element
     * @param level the current level in the elements hierarchy.
     * @param avroFields array of fields being populated
     * @throws Xsd2AvroTranslatorException
     */
    private void visit(final XmlSchema xmlSchema, XmlSchemaChoice xsdChoice,
            final int level, final ArrayNode avroFields)
            throws Xsd2AvroTranslatorException {

        log.debug("process started for choice");

        String firstAlternativeName = null;
        ArrayNode alternativeAvroFields = MAPPER.createArrayNode();

        for (XmlSchemaChoiceMember alternative : xsdChoice.getItems()) {
            if (alternative instanceof XmlSchemaElement) {
                String alternativeName = getAvroFieldName((XmlSchemaElement) alternative);
                if (firstAlternativeName == null) {
                    firstAlternativeName = alternativeName;
                }
                visit(xmlSchema, (XmlSchemaElement) alternative, level,
                        alternativeAvroFields);
            }
        }
        ObjectNode avroChoiceField = MAPPER.createObjectNode();
        avroChoiceField.put("name", firstAlternativeName + "Choice");

        ArrayNode alternativeAvroTypes = MAPPER.createArrayNode();
        for (int i = 0; i < alternativeAvroFields.size(); i++) {
            alternativeAvroTypes.add(alternativeAvroFields.get(i).get("type"));
        }

        avroChoiceField.put("type", alternativeAvroTypes);

        avroFields.add(avroChoiceField);
        log.debug("process ended for choice = " + firstAlternativeName
                + "Choice");
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
     * @throws Xsd2AvroTranslatorException if something abnormal in the xsd
     */
    private Object getAvroPrimitiveType(final String avroFieldName,
            final XmlSchemaSimpleType xsdType, boolean isArray)
            throws Xsd2AvroTranslatorException {
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
                    throw new Xsd2AvroTranslatorException(
                            "Binary type without facets in " + avroFieldName);
                }
                avroType = MAPPER.createObjectNode();
                avroType.put("type", "fixed");
                avroType.put("name", StringUtils.capitalize(avroFieldName));
                for (XmlSchemaObject facet : restriction.getFacets()) {
                    if (facet instanceof XmlSchemaMaxLengthFacet) {
                        XmlSchemaMaxLengthFacet xsef = (XmlSchemaMaxLengthFacet) facet;
                        avroType.put("size",
                                Integer.parseInt((String) xsef.getValue()));
                    }
                }
            } else if ("decimal".equals(avroPrimitiveType)) {
                if (restriction.getFacets() == null) {
                    throw new Xsd2AvroTranslatorException(
                            "Decimal type without facets in " + avroFieldName);
                }
                avroType = MAPPER.createObjectNode();
                avroType.put("type", "bytes");
                avroType.put("logicalType", "decimal");
                for (XmlSchemaFacet facet : restriction.getFacets()) {
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
     * 
     * @param xsdElement the COBOL-annotated XML schema element name.
     * @return a field name
     */
    public String getAvroFieldName(final XmlSchemaElement xsdElement) {
        return StringUtils.uncapitalize(xsdElement.getName());
    }

    /**
     * Get a name for a record type.
     * <p/>
     * TODO XML schema names might not be valid Avro type names
     * 
     * @param xsdType the XML schema complex type.
     * @return an avro type name
     */
    public String getAvroTypeName(final XmlSchemaComplexType xsdType) {
        return xsdType.getName();
    }

}
