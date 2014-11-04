package com.legstar.avro.translator;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates the Cascading Fields classes.
 * <p/>
 * Uses a simple <a
 * href="https://github.com/jknack/handlebars.java">handlebars</a> template at
 * this stage.
 * 
 */
public class Cob2AvroGenerator {

	private static Logger log = LoggerFactory
			.getLogger(Cob2AvroGenerator.class);

	public void generate(File xsdInput, File outputDir, String packageName) {

		try {
			FileUtils.forceMkdir(outputDir);

			Cob2Avro cob2Fields = new Cob2Avro();
			Map<String, String> mapSchemas = cob2Fields.translate(xsdInput,
					packageName);
			for (Entry<String, String> entry : mapSchemas.entrySet()) {

				// Write shema file
				FileUtils.writeStringToFile(new File(outputDir, entry.getKey()
						+ ".avsc"), entry.getValue());
			}

		} catch (IOException e) {
			log.error("Generation failed for input " + xsdInput.getName(), e);
		}

	}

}
