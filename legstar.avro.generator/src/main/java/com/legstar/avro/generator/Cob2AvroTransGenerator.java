package com.legstar.avro.generator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.apache.avro.generic.GenericData.StringType;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.TrueFileFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.legstar.avro.translator.Cob2Avro;
import com.legstar.coxb.cob2trans.Cob2TransException;
import com.legstar.coxb.cob2trans.Cob2TransGenerator;
import com.legstar.coxb.cob2trans.Cob2TransGenerator.Cob2TransResult;
import com.legstar.coxb.cob2trans.Cob2TransModel;

/**
 * Generates Avro specific classes based on a COBOL copybook.
 * <p/>
 * The copybook first goes through a regular legstar transformers generation.
 * <p/>
 * The intermediary COBOL-annotated XML schema produced by legstar is then translated to an Avro Schema.
 * <p/>
 * Finally the Avro compiler is invoked to produce the set of Avro specific classes.
 *
 */
public class Cob2AvroTransGenerator {

    private static final String AVSC_FILE_EXTENSION = ".avsc";

    private static final String COB2TRANS_TARGET_SUB_FOLDER = "cob2trans";

    private static final String DEFAULT_COB2TRANS_CONFIG_RESOURCE = "/cob2avrotrans.properties";

    private static final String XSD_TARGET_SUB_FOLDER = "xsd";

    private static final String JAVA_TARGET_SUB_FOLDER = "java";

    private static final String AVSC_TARGET_SUB_FOLDER = "avsc";

    private static Logger log = LoggerFactory
            .getLogger(Cob2AvroTransGenerator.class);

    public boolean generate(final Collection < File > cobolFiles, final File target) {
        return generate(cobolFiles, target, null, null);
    }

    public boolean generate(final Collection < File > cobolFiles, final File target,
            File configFile) {
        return generate(cobolFiles, target, configFile, null);
    }

    public boolean generate(final Collection < File > cobolFiles, final File target,
            File configFile, String classpath) {
        boolean rc = true;
        for (File cobolFile : cobolFiles) {
            rc &= generate(cobolFile, target, configFile, classpath);
        }
        return rc;
    }

    public boolean generate(final File cobolFile, final File target) {
        return generate(cobolFile, target, null, null);
    }

    public boolean generate(final File cobolFile, final File target,
            File configFile) {
        return generate(cobolFile, target, configFile, null);
    }

    public boolean generate(final File cobolFile, final File target,
            File configFile, String classpath) {
        try {
            log.info("COBOL to Avro transformers generation started for: {}",
                    cobolFile);

            // TODO check for preconditions
            // - input must exist
            // - JDK must be available
            String basename = Cob2TransGenerator.getBaseName(cobolFile);

            // Generate regular legstar transformers
            File cob2transTargetFolder = new File(target,
                    COB2TRANS_TARGET_SUB_FOLDER);
            Cob2AvroTransModel cob2TransAvroModel = getCob2AvroTransModel(configFile);
            Cob2TransResult result = cob2trans(cobolFile,
                    cob2transTargetFolder, classpath, cob2TransAvroModel);

            // Produce the avro schema
            File avscTargetFolder = new File(target, AVSC_TARGET_SUB_FOLDER);
            Cob2Avro cob2Avro = new Cob2Avro();
            Map < String, String > mapSchemas = cob2Avro.translate(
                    result.cob2xsdResult.xsdFile,
                    cob2TransAvroModel.getAvroPackagePrefix());
            File avscTargetFile = new File(avscTargetFolder, basename
                    + AVSC_FILE_EXTENSION);
            FileUtils.writeStringToFile(avscTargetFile,
                    mapSchemas.get(basename));

            // Gather generated sources and clean
            File xsdTargetFolder = new File(target, XSD_TARGET_SUB_FOLDER);
            FileUtils.copyFileToDirectory(result.cob2xsdResult.xsdFile,
                    xsdTargetFolder, true);
            File javaTargetFolder = new File(target, JAVA_TARGET_SUB_FOLDER);
            FileUtils.copyDirectory(new File(cob2transTargetFolder, basename
                    + "/" + cob2TransAvroModel.getSrcFolderName()),
                    javaTargetFolder, TrueFileFilter.INSTANCE);
            FileUtils.forceDeleteOnExit(cob2transTargetFolder);

            // Generate Avro specific classes
            avroCompile(avscTargetFile, javaTargetFolder);

            log.info("COBOL to Avro transformers generation complete for: {}",
                    cobolFile);

            return true;

        } catch (Exception e) {
            log.error("COBOL to Avro transformers generation failed for: {}",
                    cobolFile, e);
            return false;
        }

    }

    /**
     * Generate legstar Transformers for a single COBOL source file.
     * 
     * @param cobolFile COBOL source file
     * @param target target file or folder
     * @param classpath a java class path to use by compiler to locate
     *            dependencies
     * @param model the options in effect
     * @return intermediate and final results including a jar archive ready to
     *         deploy
     * @throws Cob2TransException if generation fails
     */
    private Cob2TransResult cob2trans(final File cobolFile, final File target,
            final String classpath, final Cob2TransModel model)
            throws Cob2TransException {
        log.info("COBOL to legstar transformers generation started for: {}",
                cobolFile);

        Cob2TransGenerator cob2trans = new Cob2TransGenerator(model);
        Cob2TransResult result = cob2trans.generate(cobolFile, target,
                classpath);

        log.info("COBOL to legstar transformers generation ended for: {}",
                cobolFile);
        if (result.cob2xsdResult.errorHistory.size() > 0) {
            log.warn("COBOL to XML schema translation produced the following errors:");
            for (String errorMessage : result.cob2xsdResult.errorHistory) {
                log.warn(errorMessage);
            }
        }
        return result;
    }

    /**
     * Given an Avro schema produce java specific classes.
     * 
     * @param avscFile the Avro schema
     * @param javaTargetFolder the target folder for java classes
     * @throws IOException if compilation fails
     */
    private void avroCompile(File avscFile, File javaTargetFolder)
            throws IOException {

        log.info("Avro compiler started for: {}", avscFile);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(avscFile);
        SpecificCompiler compiler = new CustomSpecificCompiler(schema);
        compiler.setStringType(StringType.CharSequence);
        compiler.compileToDestination(avscFile, javaTargetFolder);

        log.info("Avro compiler ended for: {}", avscFile);
    }

    private Cob2AvroTransModel getCob2AvroTransModel(File configFile)
            throws IOException {
        InputStream stream = null;
        try {
            if (configFile == null || !configFile.exists()) {
                stream = getClass().getResourceAsStream(
                        DEFAULT_COB2TRANS_CONFIG_RESOURCE);
            } else {
                stream = new FileInputStream(configFile);
            }
            Properties config = new Properties();
            config.load(stream);
            return new Cob2AvroTransModel(config);
        } finally {
            if (stream != null) {
                try {
                    stream.close();
                } catch (IOException e) {
                    log.error("Unable to close configuration stream", e);
                }
            }

        }

    }
}
