package com.legstar.avro.cob2avro.io;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FileInputStream;

import legstar.test.avro.custdat.CobolCustomerData;
import legstar.test.avro.custdat.CustomerData;

import org.apache.avro.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.legstar.avro.cob2avro.io.ZosVarRdwDatumReader;

public class ZosVarRdwDatumReaderTest {

    private static Logger log = LoggerFactory
            .getLogger(ZosVarRdwDatumReaderTest.class);

    @Test
    public void testReadCustdatFromStart() throws Exception {
        Schema schema = new Schema.Parser().parse(new File("target/gen/avsc/"
                + "custdat.avsc"));
        File inFile = new File("src/test/data/ZOS.FCUSTDAT.RDW.bin");
        ZosVarRdwDatumReader<CustomerData> datumReader = new ZosVarRdwDatumReader<CustomerData>(
                new FileInputStream(inFile), inFile.length(),
                new CobolCustomerData(), schema);
        int count = 0;
        while (datumReader.hasNext()) {
            CustomerData specific = datumReader.next();
            count++;
            logCustomerData(specific);
        }
        assertEquals(10000, count);
    }

    @Test
    public void testReadCustdatFromOffset() throws Exception {
        Schema schema = new Schema.Parser().parse(new File("target/gen/avsc/"
                + "custdat.avsc"));
        File inFile = new File("src/test/data/ZOS.FCUSTDAT.RDW.bin");
        FileInputStream is = new FileInputStream(inFile);
        
        // Read one byte (means first record read by reader is trubcated)
        is.read();
        
        ZosVarRdwDatumReader<CustomerData> datumReader = new ZosVarRdwDatumReader<CustomerData>(
                is, inFile.length() -1,
                new CobolCustomerData(), schema);
        
        // Synchronize at start of the next record
        datumReader.seekRecordStart(new CustdatZosRdwRecordMatcher());
        
        
        int count = 0;
        while (datumReader.hasNext()) {
            CustomerData specific = datumReader.next();
            count++;
            logCustomerData(specific);
        }
        assertEquals(9999, count);
        
        
        
    }


    private void logCustomerData(CustomerData specific) {
        log.info(
                "Record customer id={}, customer name={}, transaction amount={}",
                specific.getCustomerId(), specific.getPersonalData()
                        .getCustomerName(), specific.getTransactions()
                        .getTransaction().size() > 0 ? specific
                        .getTransactions().getTransaction().get(0)
                        .getTransactionAmount() : "none");

    }
}
