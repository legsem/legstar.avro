package com.legstar.avro.cob2avro.samples;

import java.io.File;

import org.apache.avro.Schema;

import com.legstar.avro.cob2avro.ZosFileDatumReader;

import com.legstar.avro.beans.custdat.bind.CustomerDataBinding;
import com.legstar.avro.specific.custdat.CustomerData;


public class CustdatReader {

    public static void main(final String[] args) throws Exception {
        Schema schema = new Schema.Parser().parse(new File("gen/avsc/"
                + "custdat.avsc"));
        int count = 0;
        ZosFileDatumReader < CustomerData > reader = new ZosFileDatumReader < CustomerData >(
                new File("data/ZOS.FCUSTDAT.RDW.bin"), true, schema,
                new CustomerDataBinding());

        while (reader.hasNext()) {
            CustomerData specific = reader.next();
            count++;
            System.out.println("Record num=" + count + " customer id=" + specific
                    .getPersonalData().getCustomerName());
        }

    }

}
