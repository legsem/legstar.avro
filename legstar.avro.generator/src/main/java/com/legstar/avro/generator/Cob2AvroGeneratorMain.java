package com.legstar.avro.generator;

import java.io.File;
import java.util.Properties;

import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.legstar.base.generator.AbstractCob2JavaGeneratorMain;

/**
 * Exposes the {@link Cob2AvroGenerator} utility as a command line tool.
 * 
 */
public class Cob2AvroGeneratorMain extends AbstractCob2JavaGeneratorMain {


    private static Logger log = LoggerFactory
            .getLogger(Cob2AvroGeneratorMain.class);


    /**
     * @param args translator options. Provides help if no arguments passed.
     */
    public static void main(final String[] args) {
        Cob2AvroGeneratorMain main = new Cob2AvroGeneratorMain();
        main.execute(args);
    }

    public void generate(Properties configProps, File cobolFile,
            String cobolFileEncoding, File output, String packageNamePrefix,
            final String xsltFileName) {
        log.info("Processing COBOL file " + cobolFile);
        Cob2AvroGenerator gen = new Cob2AvroGenerator(
                configProps);
        String baseName = FilenameUtils.getBaseName(
                cobolFile.getAbsolutePath()).toLowerCase();
        String packageName = packageNamePrefix == null ? baseName
                : (packageNamePrefix + "." + baseName);

        gen.generate(cobolFile,
                cobolFileEncoding, output, packageName, xsltFileName);
    }

}
