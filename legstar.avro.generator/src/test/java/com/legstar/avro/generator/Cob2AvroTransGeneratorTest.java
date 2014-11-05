package com.legstar.avro.generator;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class Cob2AvroTransGeneratorTest {
    
    private static final File COBOL_SOURCE_FOLDER = new File("src/test/cobol");
    
    private static final File TARGET_FOLDER = new File("target/test/generator");
    
    @Before
    public void setUp() throws Exception {
        FileUtils.forceMkdir(TARGET_FOLDER);
        FileUtils.cleanDirectory(TARGET_FOLDER);
    }

    @Test
    public void testFlat01() throws Exception {
        Cob2AvroTransGenerator generator = new Cob2AvroTransGenerator();
        generator.generate(new File(COBOL_SOURCE_FOLDER, "FLAT01"), TARGET_FOLDER);
    }
}
