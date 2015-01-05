package com.legstar.avro.cob2avro;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.rules.TestName;

public class AbstractTest {

    /** This means references should be created instead of compared to results. */
    private boolean createReferences = false;

    @Rule
    public TestName name = new TestName();

    /** Generated classes Reference folder. */
    public static final File SRC_REF_DIR = new File(
            "src/test/resources/references");

    /**
     * Check a string result against a reference file content (One ref file per
     * test case).
     * <p/>
     * If reference needs to be created, it is created rather than used for
     * comparison.
     * 
     * @param resultText the result text
     * @param refFileName a reference file name
     * @throws Exception if IO fails
     */
    public void check(final String resultText, String refFileName) {
        try {
            File refFile = new File(SRC_REF_DIR, getClass().getSimpleName() + "/" + name.getMethodName() + "/"
                    + refFileName);
            if (isCreateReferences()) {
                FileUtils.writeStringToFile(refFile, resultText, Charsets.UTF_8);
            } else {
                String referenceText = FileUtils.readFileToString(refFile,
                        Charsets.UTF_8);
                assertEquals(referenceText, resultText);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * @return true if references should be created instead of compared to
     *         results
     */
    public boolean isCreateReferences() {
        return createReferences;
    }

    public void setCreateReferences(boolean createReferences) {
        this.createReferences = createReferences;
    }

}
