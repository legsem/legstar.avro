package com.legstar.avro.translator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.ws.commons.schema.XmlSchema;
import org.apache.ws.commons.schema.XmlSchemaCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Translates COBOL structures to <a href="http://avro.apache.org/">apache
 * avro</a> json schemas.
 * <p/>
 * Assumes the COBOL structures were already transformed to legstar
 * COBOL-annotated xml schemas which serve as input here.
 * 
 */
public class Cob2Avro {

	private static final String LEGSTAR_XSD_FILE_ENCODING = "UTF-8";

	private static Logger log = LoggerFactory.getLogger(Cob2Avro.class);

	/**
	 * Translate a legstar generated xsd file (with COBOL annotations) to an
	 * avro schema.
	 * <p/>
	 * If a folder is passed rather than a single file then all xsd files from
	 * that folder are translated.
	 * 
	 * @param xsdInput a single xsd file or a folder containing xsd files
	 * @param avroNamespacePrefix for avro schema (package name for generated objects)
	 * @return for each xsd file translated, the avro schema serialized as
	 *         string
	 * @throws IOException if translation fails
	 */
	public Map<String, String> translate(File xsdInput, String avroNamespacePrefix) throws IOException {

		log.info("COBOL to Avro translator started");
		Map<String, String> mapSchemas = new HashMap<String, String>();

		if (!xsdInput.exists()) {
			throw new IOException("Specified input '" + xsdInput.getName()
					+ "' does not exist");
		}

		log.info("Reading legstar xsd files from " + xsdInput.getName());

		if (xsdInput.isDirectory()) {
			Collection<File> xsdFiles = FileUtils.listFiles(xsdInput,
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

	private void translate(File xsdFile, Map<String, String> mapSchemas,
			String avroNamespacePrefix) throws IOException {
		String xsdFileName = FilenameUtils.getBaseName(xsdFile.getName());
		String schema = translate(new InputStreamReader(new FileInputStream(
				xsdFile), LEGSTAR_XSD_FILE_ENCODING), xsdFileName, avroNamespacePrefix);
		mapSchemas.put(xsdFileName, schema);
		log.info("LegStar xsd {} translated to schema {}", xsdFileName, schema);
	}

	public String translate(Reader reader, String xsdFileName, String avroNamespacePrefix)
			throws Cob2AvroException {
		XmlSchemaCollection schemaCol = new XmlSchemaCollection();
		XmlSchema xsd = schemaCol.read(reader, null);
		Cob2AvroVisitor visitor = new Cob2AvroVisitor(xsd, xsdFileName,
		        avroNamespacePrefix);
		visitor.visit();
		return visitor.getAvroSchemaAsString();
	}

}
