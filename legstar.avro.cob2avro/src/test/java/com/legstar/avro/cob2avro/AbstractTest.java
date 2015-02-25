package com.legstar.avro.cob2avro;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
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

    public Schema getSchema(String casename) {
        try {
            return new Schema.Parser().parse(new File("target/gen/avsc/"
                    + casename + ".avsc"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String avro2Json(IndexedRecord avroRecord) {
        try {
            DatumWriter < IndexedRecord > datumWriter = new GenericDatumWriter < IndexedRecord >(
                    avroRecord.getSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().jsonEncoder(
                    avroRecord.getSchema(), out, true);
            datumWriter.write(avroRecord, encoder);
            encoder.flush();
            out.close();
            return new String(out.toByteArray(), "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public byte[] avroSerializeGeneric(GenericRecord genericRecord) {
        try {
            DatumWriter < GenericRecord > datumWriter = new GenericDatumWriter < GenericRecord >(
                    genericRecord.getSchema());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Encoder encoder = EncoderFactory.get().directBinaryEncoder(out,
                    null);
            datumWriter.write(genericRecord, encoder);
            return out.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public <T> void avroReadSpecific(Class < T > clazz, byte[] data) {
        try {
            DatumReader < T > datumReader = new SpecificDatumReader < T >(clazz);
            Object decoded = datumReader.read(null, DecoderFactory.get()
                    .binaryDecoder(data, null));
            System.out.println(decoded);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
