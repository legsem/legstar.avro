package com.legstar.avro.generator;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.commons.compress.utils.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.legstar.avro.translator.Xsd2AvroTranslator;
import com.legstar.base.generator.Xsd2CobolTypesGenerator;
import com.legstar.base.utils.NamespaceUtils;
import com.legstar.cob2xsd.Cob2Xsd;
import com.legstar.cob2xsd.Cob2XsdConfig;

/**
 * Generates Avro specific classes based on a COBOL copybook.
 * <p/>
 * The copybook first goes through a regular legstar COBOL to XML Schema
 * translation.
 * <p/>
 * The COBOL-annotated XML schema is then translated to an Avro Schema.
 * <p/>
 * The COBOL-annotated XML schema is used to produce regular LegStar converter
 * support classes.
 * <p/>
 * Finally the Avro compiler is invoked to produce a set of Avro converter
 * support classes.
 * 
 */
public class Cob2AvroGenerator {

    private static final String AVSC_FILE_EXTENSION = ".avsc";

    private static final String XSD_FILE_EXTENSION = ".xsd";

    private static final String JAVA_FILE_EXTENSION = ".java";

    private static final String XSD_TARGET_SUB_FOLDER = "xsd";

    private static final String JAVA_TARGET_SUB_FOLDER = "java";

    private static final String AVSC_TARGET_SUB_FOLDER = "avsc";

    private final Cob2Xsd cob2xsd;

    private final Xsd2CobolTypesGenerator xsd2CobolTypes;

    private final Xsd2AvroTranslator cob2AvroTranslator;
    
    /** All output files are in UTF-8. Might want to externalize as a parameter.*/
    private static final Charset TARGET_FILES_ENCODING = Charsets.UTF_8;

    private static Logger log = LoggerFactory
            .getLogger(Cob2AvroGenerator.class);

    public Cob2AvroGenerator(Properties configProps) {
        cob2xsd = new Cob2Xsd(new Cob2XsdConfig(configProps));
        cob2AvroTranslator = new Xsd2AvroTranslator();
        xsd2CobolTypes = new Xsd2CobolTypesGenerator();
    }

    /**
     * Given a COBOL copybook, produce a set of java classes (source code) used
     * to convert mainframe data (matching the copybook) at runtime.
     * 
     * @param cobolFile the COBOL copybook file
     * @param cobolFileEncoding the COBOL copybook file character encoding
     * @param targetFolder the target folder
     * @param targetPackageName the java package the generated classes should
     *            reside in
     * @param xsltFileName an optional xslt to apply on the XML Schema
     * @return a map of java class names to their source code
     */
    public boolean generate(File cobolFile, String cobolFileEncoding,
            final File targetFolder, String targetPackageName,
            final String xsltFileName) {
        try {
            log.info("COBOL to Avro generation for: {}", cobolFile);
            Reader reader = cobolFileEncoding == null ? new InputStreamReader(
                    new FileInputStream(cobolFile)) : new InputStreamReader(
                    new FileInputStream(cobolFile), cobolFileEncoding);
            String targetSchemaName = FilenameUtils.getBaseName(cobolFile
                    .getName().toLowerCase());
            return generate(reader, targetFolder, targetPackageName,
                    targetSchemaName, xsltFileName);
        } catch (UnsupportedEncodingException e) {
            throw new Cob2AvroGeneratotException(e);
        } catch (FileNotFoundException e) {
            throw new Cob2AvroGeneratotException(e);
        }
    }

    /**
     * Generate the LegStar Avro artifacts.
     * <p/>
     * The process goes through the following steps:
     * <ul>
     * <li>Translate the COBOL copybook to XML Schema</li>
     * <li>Generate LegStar converter support classes</li>
     * <li>Produce the avro schema</li>
     * <li>Parse and compile the Avro schema</li>
     * <li>Generate Avro specific classes</li>
     * </ul>
     * 
     * @param cobolReader
     * @param targetFolder
     * @param targetPackageName
     * @param targetSchemaName
     * @param xsltFileName
     * @return
     */
    public boolean generate(Reader cobolReader, final File targetFolder,
            String targetPackageName, String targetSchemaName,
            final String xsltFileName) {
        try {

            String xmlSchemaSource = generateXmlSchema(cobolReader,
                    NamespaceUtils.toNamespace(targetPackageName),
                    targetSchemaName, xsltFileName);
            serializeToFile(targetFolder, XSD_TARGET_SUB_FOLDER,
                    targetSchemaName, XSD_FILE_EXTENSION, xmlSchemaSource);

            Map < String, String > codeMap = generateConverterClasses(
                    xmlSchemaSource, targetPackageName);
            String subFolder = targetPackageName == null ? ""
                    : (targetPackageName.replace(".", "/") + "/");
            for (Entry < String, String > entry : codeMap.entrySet()) {
                serializeToFile(targetFolder, JAVA_TARGET_SUB_FOLDER, subFolder
                        + entry.getKey(), JAVA_FILE_EXTENSION, entry.getValue());
            }

            String avroSchemaSource = generateAvroSchema(xmlSchemaSource,
                    targetPackageName, targetSchemaName);
            File avroSchemaFile = serializeToFile(targetFolder,
                    AVSC_TARGET_SUB_FOLDER, targetSchemaName,
                    AVSC_FILE_EXTENSION, avroSchemaSource);

            avroCompile(avroSchemaSource, avroSchemaFile, new File(
                    targetFolder, JAVA_TARGET_SUB_FOLDER));

            log.info("COBOL to Avro generation succeeded");
            return true;

        } catch (Exception e) {
            log.error("COBOL to Avro generation failed for: {}",
                    targetSchemaName, e);
            return false;
        }

    }

    /**
     * Translate the COBOL copybook to XML Schema.
     * 
     * @param cobolReader the input COBOL copybook (as a reader)
     * @param targetNamespace the target XML schema namespace
     * @param targetXmlSchemaName the target XML schema name
     * @param xsltFileName an optional XSLT to apply on the XML schema
     * @return the XML schema source
     */
    private String generateXmlSchema(Reader cobolReader,
            String targetNamespace, String targetXmlSchemaName,
            final String xsltFileName) {

        log.debug("XML schema {} generation started", targetXmlSchemaName
                + XSD_FILE_EXTENSION);
        String xmlSchemaSource = cob2xsd.translate(cobolReader,
                NamespaceUtils.toNamespace(targetNamespace), xsltFileName);
        if (log.isDebugEnabled()) {
            log.debug("Generated Cobol-annotated XML Schema: ");
            log.debug(xmlSchemaSource);
        }
        log.debug("XML schema {} generation ended", targetXmlSchemaName
                + XSD_FILE_EXTENSION);
        return xmlSchemaSource;

    }

    /**
     * Produce the LegStar converter support classes.
     * 
     * @param xmlSchemaSource the XML schema source
     * @param targetPackageName the target Avro package name
     * @return a map of lass names associated with their code
     */
    private Map < String, String > generateConverterClasses(
            String xmlSchemaSource, String targetPackageName) {

        log.debug("Converter support classes generation started");
        Map < String, String > codeMap = xsd2CobolTypes.generate(
                new StringReader(xmlSchemaSource), targetPackageName);
        if (log.isDebugEnabled()) {
            log.debug("Generated converter support classes: ");
            for (String code : codeMap.values()) {
                log.debug(code);
                log.debug("\n");
            }
        }
        log.debug("Converter support classes generation ended");
        return codeMap;
    }

    private File serializeToFile(final File target, String targetSubFolder,
            String fileName, String extension, String source)
            throws IOException {
        File targetFolder = new File(target, targetSubFolder);
        File targetFile = new File(targetFolder, fileName + extension);
        FileUtils.writeStringToFile(targetFile, source, TARGET_FILES_ENCODING);
        return targetFile;
    }

    /**
     * Produce the avro schema.
     * 
     * @param xmlSchemaSource the XML schema source
     * @param targetPackageName the target Avro package name
     * @param targetAvroSchemaName the target Avro schema name
     * @return the generated Avro schema
     * @throws IOException if serialization fails
     */
    private String generateAvroSchema(String xmlSchemaSource,
            String targetPackageName, String targetAvroSchemaName)
            throws IOException {

        log.debug("Avro schema {} generation started", targetAvroSchemaName
                + AVSC_FILE_EXTENSION);
        String avroSchemaSource = cob2AvroTranslator.translate(
                new StringReader(xmlSchemaSource), targetPackageName,
                targetAvroSchemaName);
        if (log.isDebugEnabled()) {
            log.debug("Generated Avro Schema: ");
            log.debug(avroSchemaSource);
        }
        log.debug("Avro schema {} generation ended", targetAvroSchemaName
                + AVSC_FILE_EXTENSION);
        return avroSchemaSource;
    }

    /**
     * Given an Avro schema produce java specific classes.
     * 
     * @param avroSchemaFile the Avro schema file (used by avro for timestamp
     *            checking)
     * @param avroSchemaSource the Avro schema source
     * @param javaTargetFolder the target folder for java classes
     * @throws IOException if compilation fails
     */
    private void avroCompile(String avroSchemaSource, File avroSchemaFile,
            File javaTargetFolder) throws IOException {

        log.debug("Avro compiler started for: {}", avroSchemaFile);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(avroSchemaSource);
        SpecificCompiler compiler = new CustomSpecificCompiler(schema);
        compiler.setStringType(StringType.CharSequence);
        compiler.setOutputCharacterEncoding(TARGET_FILES_ENCODING.name());
        compiler.compileToDestination(avroSchemaFile, javaTargetFolder);

        log.debug("Avro compiler ended for: {}", avroSchemaFile);
    }

}
