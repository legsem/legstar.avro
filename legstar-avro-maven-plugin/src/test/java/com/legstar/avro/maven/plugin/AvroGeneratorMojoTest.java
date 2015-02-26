package com.legstar.avro.maven.plugin;

import java.io.File;

import org.apache.maven.plugin.testing.AbstractMojoTestCase;

import com.legstar.avro.maven.plugin.AvroGeneratorMojo;

public class AvroGeneratorMojoTest extends AbstractMojoTestCase {

    protected void setUp() throws Exception {
        super.setUp();
    }

    public void testMojoBasic() throws Exception {
        File testPom = new File(getBasedir(),
                "src/test/resources/poms/basic-test-plugin-config.xml");

        AvroGeneratorMojo mojo = (AvroGeneratorMojo) lookupMojo("generate-avro", testPom);
        mojo.execute();
        assertTrue(new File(
                "target/generated-sources/java/flat01/CobolFlat01Record.java")
                .exists());
        assertTrue(new File(
                "target/generated-sources/java/flat01/Flat01Record.java")
                .exists());
        assertTrue(new File(
                "target/generated-sources/avsc/flat01.avsc")
                .exists());
        assertTrue(new File(
                "target/generated-sources/xsd/flat01.xsd")
                .exists());
    }

    public void testMojoComplete() throws Exception {
        File testPom = new File(getBasedir(),
                "src/test/resources/poms/complete-test-java-plugin-config.xml");

        AvroGeneratorMojo mojo = (AvroGeneratorMojo) lookupMojo("generate-avro", testPom);
        mojo.execute();
        assertTrue(new File(
                "target/generated-test-sources/java/com/example/flat01/CobolFlat01Record.java")
                .exists());
        assertTrue(new File(
                "target/generated-test-sources/java/com/example/flat01/Flat01Record.java")
                .exists());
        assertTrue(new File(
                "target/generated-test-sources/avsc/flat01.avsc")
                .exists());
        assertTrue(new File(
                "target/generated-test-sources/xsd/flat01.xsd")
                .exists());
    }

}
