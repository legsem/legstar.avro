package com.legstar.avro.generator;

import static org.junit.Assert.*;

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
        assertFalse(new File(TARGET_FOLDER, "avsc/flat01.avsc").exists());
        assertFalse(new File(TARGET_FOLDER, "java/com/legstar/avro/specific/flat01/Flat01Record.java").exists());
        assertFalse(new File(TARGET_FOLDER, "java/com/legstar/avro/beans/flat01/bind/Flat01RecordBinding.java").exists());

        Cob2AvroTransGenerator generator = new Cob2AvroTransGenerator();
        generator.generate(new File(COBOL_SOURCE_FOLDER, "FLAT01"), TARGET_FOLDER);

        assertTrue(new File(TARGET_FOLDER, "avsc/flat01.avsc").exists());
        assertTrue(new File(TARGET_FOLDER, "java/com/legstar/avro/specific/flat01/Flat01Record.java").exists());
        assertTrue(new File(TARGET_FOLDER, "java/com/legstar/avro/beans/flat01/bind/Flat01RecordBinding.java").exists());
    }
}
