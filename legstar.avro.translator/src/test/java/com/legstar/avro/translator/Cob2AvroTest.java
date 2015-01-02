package com.legstar.avro.translator;

import static org.junit.Assert.*;

import java.io.File;
import java.util.Map;

import org.apache.avro.Schema;
import org.junit.Before;
import org.junit.Test;

public class Cob2AvroTest extends AbstractTest {

    private static final boolean CREATE_REFERENCE = true;

    @Before
    public void setUp() throws Exception {
        setCreateReferences(CREATE_REFERENCE);
    }

    @Test
	public void testFlat01() throws Exception {
		Cob2Avro cob2Avro = new Cob2Avro();
		Map<String, String> mapSchemas = cob2Avro.translate(new File(
				TEST_XSD_FOLDER, "flat01.xsd"), "legstar.avro.test");
		assertEquals(1, mapSchemas.size());
		check(mapSchemas.get("flat01"), "flat01.avsc");
        checkSchema(mapSchemas.get("flat01"), 3);
	}

    @Test
	public void testFlat02() throws Exception {
		Cob2Avro cob2Avro = new Cob2Avro();
		Map<String, String> mapSchemas = cob2Avro.translate(new File(
				TEST_XSD_FOLDER, "flat02.xsd"), "legstar.avro.test");
		assertEquals(1, mapSchemas.size());
		check(mapSchemas.get("flat02"), "flat02.avsc");
        checkSchema(mapSchemas.get("flat02"), 4);
	}

    @Test
	public void testStru03() throws Exception {
		Cob2Avro cob2Avro = new Cob2Avro();
		Map<String, String> mapSchemas = cob2Avro.translate(new File(
				TEST_XSD_FOLDER, "stru03.xsd"), "legstar.avro.test");
		assertEquals(1, mapSchemas.size());
		check(mapSchemas.get("stru03"), "stru03.avsc");
        checkSchema(mapSchemas.get("stru03"), 4);
	}

    @Test
	public void testStru04() throws Exception {
		Cob2Avro cob2Avro = new Cob2Avro();
		Map<String, String> mapSchemas = cob2Avro.translate(new File(
				TEST_XSD_FOLDER, "stru04.xsd"), "legstar.avro.test");
		assertEquals(1, mapSchemas.size());
		check(mapSchemas.get("stru04"), "stru04.avsc");
        checkSchema(mapSchemas.get("stru04"), 3);
	}

    @Test
    public void testStru05() throws Exception {
        Cob2Avro cob2Avro = new Cob2Avro();
        Map<String, String> mapSchemas = cob2Avro.translate(new File(
                TEST_XSD_FOLDER, "stru05.xsd"), "legstar.avro.test");
        assertEquals(1, mapSchemas.size());
        check(mapSchemas.get("stru05"), "stru05.avsc");
        checkSchema(mapSchemas.get("stru05"), 3);
    }

    @Test
	public void testAlltypes() throws Exception {
		Cob2Avro cob2Avro = new Cob2Avro();
		Map<String, String> mapSchemas = cob2Avro.translate(new File(
				TEST_XSD_FOLDER, "alltypes.xsd"), "legstar.avro.test");
		assertEquals(1, mapSchemas.size());
		check(mapSchemas.get("alltypes"), "alltypes.avsc");
        checkSchema(mapSchemas.get("alltypes"), 26);
	}

    @Test
    public void testRdef01() throws Exception {
        Cob2Avro cob2Avro = new Cob2Avro();
        Map<String, String> mapSchemas = cob2Avro.translate(new File(
                TEST_XSD_FOLDER, "rdef01.xsd"), "legstar.avro.test");
        assertEquals(1, mapSchemas.size());
        check(mapSchemas.get("rdef01"), "rdef01.avsc");
        checkSchema(mapSchemas.get("rdef01"), 2);
    }
    
    @Test
    public void testRdef02() throws Exception {
        Cob2Avro cob2Avro = new Cob2Avro();
        Map<String, String> mapSchemas = cob2Avro.translate(new File(
                TEST_XSD_FOLDER, "rdef02.xsd"), "legstar.avro.test");
        assertEquals(1, mapSchemas.size());
        check(mapSchemas.get("rdef02"), "rdef02.avsc");
        checkSchema(mapSchemas.get("rdef02"), 3);
    }
    
    @Test
    public void testCusdat() throws Exception {
        Cob2Avro cob2Avro = new Cob2Avro();
        Map<String, String> mapSchemas = cob2Avro.translate(new File(
                TEST_XSD_FOLDER, "cusdat.xsd"), "legstar.avro.test");
        assertEquals(1, mapSchemas.size());
        check(mapSchemas.get("cusdat"), "cusdat.avsc");
        checkSchema(mapSchemas.get("cusdat"), 3);
    }
    
    @Test
    public void testCflt01() throws Exception {
        Cob2Avro cob2Avro = new Cob2Avro();
        Map<String, String> mapSchemas = cob2Avro.translate(new File(
                TEST_XSD_FOLDER, "cflt01.xsd"), "legstar.avro.test");
        assertEquals(1, mapSchemas.size());
        check(mapSchemas.get("cflt01"), "cflt01.avsc");
        checkSchema(mapSchemas.get("cflt01"), 2);
    }
    
    private void checkSchema(String schemaSource, int expectedFields) throws Exception {
        Schema schema = new Schema.Parser().parse(schemaSource);
        assertEquals(expectedFields, schema.getFields().size());
    }

}
