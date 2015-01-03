package com.legstar.avro.generator;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Before;
import org.junit.Test;

import com.legstar.cob2xsd.Cob2XsdConfig;

public class Cob2AvroGeneratorTest extends AbstractTest {

    private static final boolean CREATE_REFERENCE = false;

    private static final String LEGSTAR_COBOL_FILE_ENCODING = "UTF-8";

    private static final File OUTPUT_DIR = new File(
            "target/generated-test-sources");

    private Cob2AvroGenerator gen;

    @Before
    public void setUp() throws Exception {
        setCreateReferences(CREATE_REFERENCE);
        gen = new Cob2AvroGenerator(Cob2XsdConfig.getDefaultConfigProps());
        FileUtils.forceMkdir(OUTPUT_DIR);
    }

    @Test
    public void testAlltypesGenerate() throws Exception {
        generateAndCheck("ALLTYPES");
    }

    @Test
    public void testCflt01Generate() throws Exception {
        generateAndCheck("CFLT01");
    }

    @Test
    public void testCustdatGenerate() throws Exception {
        generateAndCheck("CUSTDAT", "CustomerData");
    }

    @Test
    public void testFlat01Generate() throws Exception {
        generateAndCheck("FLAT01");
    }

    @Test
    public void testFlat02Generate() throws Exception {
        generateAndCheck("FLAT02");
    }

    @Test
    public void testRdef01Generate() throws Exception {
        generateAndCheck("RDEF01");
    }

    @Test
    public void testRdef02Generate() throws Exception {
        generateAndCheck("RDEF02");
    }

    @Test
    public void testStru01Generate() throws Exception {
        generateAndCheck("STRU01");
    }

    @Test
    public void testStru03Generate() throws Exception {
        generateAndCheck("STRU03");
    }

    @Test
    public void testStru04Generate() throws Exception {
        generateAndCheck("STRU04");
    }

    @Test
    public void testStru051Generate() throws Exception {
        generateAndCheck("STRU05");
    }

    private void generateAndCheck(String programName) throws Exception {
        generateAndCheck(programName,
                StringUtils.capitalize(getSchemaName(programName)) + "Record");
    }

    private void generate(String programName) {
        File cobolFile = new File(TEST_COBOL_FOLDER, programName);
        gen.generate(cobolFile, LEGSTAR_COBOL_FILE_ENCODING, OUTPUT_DIR,
                "test.example", null);
    }

    private void generateAndCheck(String programName, String recordName)
            throws Exception {
        generate(programName);
        checkFiles(getSchemaName(programName), recordName);
    }

    private String getSchemaName(String programName) {
        return programName.toLowerCase();
    }

    private void checkFiles(String schemaName, String recordName)
            throws Exception {
        check(getCode(recordName), recordName + ".java");
        assertTrue(getJavaFile("Cobol" + recordName).exists());
        assertTrue(getAvscFile(schemaName).exists());
        assertTrue(getXsdFile(schemaName).exists());

    }

    private String getCode(String className) throws IOException {
        return FileUtils.readFileToString(getJavaFile(className));
    }

    private File getJavaFile(String className) {
        return new File(OUTPUT_DIR, "java/test/example/" + className + ".java");
    }

    private File getAvscFile(String schemaName) {
        return new File(OUTPUT_DIR, "avsc/" + schemaName + ".avsc");
    }

    private File getXsdFile(String schemaName) {
        return new File(OUTPUT_DIR, "xsd/" + schemaName + ".xsd");
    }
}
