package com.legstar.avro.cob2avro;

import static org.junit.Assert.*;

import java.io.File;

import org.apache.avro.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import legstar.avro.test.beans.cusdat.bind.CustomerDataBinding;
import legstar.avro.test.specific.cusdat.CustomerData;

public class ZosFileDatumReaderTest {

    private static Logger log = LoggerFactory
            .getLogger(ZosFileDatumReaderTest.class);
    
    @Test
    public void testReadCustdat() throws Exception {
        Schema schema = new Schema.Parser().parse(new File("target/gen/avsc/"
                + "cusdat.avsc"));
        int count = 0;
        ZosFileDatumReader < CustomerData > reader = new ZosFileDatumReader < CustomerData >(
                new File("src/test/data/ZOS.FCUSTDAT.bin"), false, schema,
                new CustomerDataBinding());

        while (reader.hasNext()) {
            CustomerData specific = reader.next();
            count++;
            log.info("Record num={} customer id={}", count, specific
                    .getPersonalData().getCustomerName());
        }
        assertEquals(10000, count);

    }

    @Test
    public void testReadCustdatWithRdw() throws Exception {
        Schema schema = new Schema.Parser().parse(new File("target/gen/avsc/"
                + "cusdat.avsc"));
        int count = 0;
        ZosFileDatumReader < CustomerData > reader = new ZosFileDatumReader < CustomerData >(
                new File("src/test/data/ZOS.FCUSTDAT.RDW.bin"), true, schema,
                new CustomerDataBinding());

        while (reader.hasNext()) {
            CustomerData specific = reader.next();
            count++;
            log.info("Record num={} customer id={}", specific.getCustomerId(),
                    specific.getPersonalData().getCustomerName());
        }
        assertEquals(10000, count);

    }

}
