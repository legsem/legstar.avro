package com.legstar.avro.cob2avro;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.Hashtable;

import legstar.avro.test.beans.rdef01.Rdef01Record;
import legstar.avro.test.beans.rdef01.bind.Rdef01RecordBinding;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

import com.legstar.coxb.CobolElementVisitor;
import com.legstar.coxb.ICobolBinding;
import com.legstar.coxb.ICobolChoiceBinding;
import com.legstar.coxb.ICobolComplexBinding;
import com.legstar.coxb.ICobolUnmarshalChoiceStrategy;
import com.legstar.coxb.host.HostData;
import com.legstar.coxb.host.HostException;

public class Cob2AvroUnmarshalVisitorTest {

    private File avroFiles;

    @Before
    public void setUp() throws Exception {
        avroFiles = new File("target/test/files");
        FileUtils.forceMkdir(avroFiles);
        FileUtils.cleanDirectory(avroFiles);
    }

    @Test
    public void testCob2AvroFlat01() throws Exception {

        GenericRecord genericRecord = toGenericRecord("flat01",
                "F0F0F0F0F4F3D5C1D4C5F0F0F0F0F4F3404040404040404040400215000F",
                new legstar.avro.test.beans.flat01.bind.Flat01RecordBinding());

        assertEquals(43l, genericRecord.get("comNumber"));
        assertEquals("NAME000043", genericRecord.get("comName"));
        assertEquals(
                new BigDecimal("2150.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) genericRecord.get("comAmount"), 2));

        File file = avroSaveGeneric("flat01", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.flat01.Flat01Record.class,
                file);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCob2AvroFlat02() throws Exception {

        GenericRecord genericRecord = toGenericRecord(
                "flat02",
                "F0F0F0F0F6F2D5C1D4C5F0F0F0F0F6F2404040404040404040400310000F003E001F0014000F000C",
                new legstar.avro.test.beans.flat02.bind.Flat02RecordBinding());

        assertEquals(62l, genericRecord.get("comNumber"));
        assertEquals("NAME000062", genericRecord.get("comName"));
        assertEquals(
                new BigDecimal("3100.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) genericRecord.get("comAmount"), 2));
        GenericArray < Integer > genericArray = (GenericArray < Integer >) genericRecord
                .get("comArray");
        assertEquals(62, (int) genericArray.get(0));
        assertEquals(31, (int) genericArray.get(1));
        assertEquals(20, (int) genericArray.get(2));
        assertEquals(15, (int) genericArray.get(3));
        assertEquals(12, (int) genericArray.get(4));

        File file = avroSaveGeneric("flat02", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.flat02.Flat02Record.class,
                file);

    }

    @Test
    public void testCob2AvroStru01() throws Exception {

        GenericRecord genericRecord = toGenericRecord(
                "stru01",
                "F0F0F0F0F6F2D5C1D4C5F0F0F0F0F6F2404040404040404040400310000F003EC1C2",
                new legstar.avro.test.beans.stru01.bind.Stru01RecordBinding());

        assertEquals(62l, genericRecord.get("comNumber"));
        assertEquals("NAME000062", genericRecord.get("comName"));
        assertEquals(
                new BigDecimal("3100.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) genericRecord.get("comAmount"), 2));
        GenericRecord subRecord = (GenericRecord) genericRecord
                .get("comSubRecord");
        assertEquals(62, subRecord.get("comItem1"));
        assertEquals("AB", subRecord.get("comItem2"));

        File file = avroSaveGeneric("stru01", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.stru01.Stru01Record.class,
                file);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCob2AvroStru03() throws Exception {

        GenericRecord genericRecord = toGenericRecord(
                "stru03",
                "F0F0F0F0F6F2D5C1D4C5F0F0F0F0F6F2404040404040404040400310000F003EC1C2001FC1C20014C1C2000FC1C2000CC1C2",
                new legstar.avro.test.beans.stru03.bind.Stru03RecordBinding());

        assertEquals(62l, genericRecord.get("comNumber"));
        assertEquals("NAME000062", genericRecord.get("comName"));
        assertEquals(
                new BigDecimal("3100.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) genericRecord.get("comAmount"), 2));
        GenericArray < GenericRecord > genericArray = (GenericArray < GenericRecord >) genericRecord
                .get("comArray");
        assertEquals(62, genericArray.get(0).get("comItem1"));
        assertEquals("AB", genericArray.get(0).get("comItem2"));
        assertEquals(31, genericArray.get(1).get("comItem1"));
        assertEquals("AB", genericArray.get(1).get("comItem2"));
        assertEquals(20, genericArray.get(2).get("comItem1"));
        assertEquals("AB", genericArray.get(2).get("comItem2"));
        assertEquals(15, genericArray.get(3).get("comItem1"));
        assertEquals("AB", genericArray.get(3).get("comItem2"));
        assertEquals(12, genericArray.get(4).get("comItem1"));
        assertEquals("AB", genericArray.get(4).get("comItem2"));

        File file = avroSaveGeneric("stru03", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.stru03.Stru03Record.class,
                file);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCob2AvroStru04() throws Exception {

        GenericRecord genericRecord = toGenericRecord(
                "stru04",
                "0190000F00090006C2C5C5C2C4C40001900FC2C2C5C4C5C30000950F0003000000020013000CC2C4C2C1C5C40003800FC1C5C2C2C4C10001900F000600000005001C0013C1C5C2C5C1C30005700FC4C2C3C3C3C20002850F0009000000080023750F",
                new legstar.avro.test.beans.stru04.bind.Stru04RecordBinding());

        assertEquals(
                new BigDecimal("1900.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) genericRecord.get("comItem1"), 2));
        GenericArray < GenericRecord > commArray1 = (GenericArray < GenericRecord >) genericRecord
                .get("comArray1");

        /* -- Start Array 1, item 0*/
        assertEquals(9, commArray1.get(0).get("comItem2"));
        GenericRecord commGroup1_0 = (GenericRecord) commArray1.get(0).get(
                "comGroup1");
        assertEquals(6, commGroup1_0.get("comItem3"));
        GenericArray < GenericRecord > commArray2_0 = (GenericArray < GenericRecord >) commGroup1_0
                .get("comArray2");
        /* ---- Start Array 2, item 0*/
        assertEquals("B", commArray2_0.get(0).get("comItem4"));
        GenericArray < String > commArray3_0_0 = (GenericArray < String >) commArray2_0
                .get(0).get("comArray3");
        assertEquals("E", commArray3_0_0.get(0));
        assertEquals("E", commArray3_0_0.get(1));
        assertEquals("B", commArray3_0_0.get(2));
        assertEquals("D", commArray3_0_0.get(3));
        assertEquals("D", commArray3_0_0.get(4));
        assertEquals(
                new BigDecimal("19.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) commArray2_0.get(0).get("comItem5"), 2));
        /* ---- End Array 2, item 0*/

        /* ---- Start Array 2, item 1*/
        assertEquals("B", commArray2_0.get(1).get("comItem4"));
        GenericArray < String > commArray3_0_1 = (GenericArray < String >) commArray2_0
                .get(1).get("comArray3");
        assertEquals("B", commArray3_0_1.get(0));
        assertEquals("E", commArray3_0_1.get(1));
        assertEquals("D", commArray3_0_1.get(2));
        assertEquals("E", commArray3_0_1.get(3));
        assertEquals("C", commArray3_0_1.get(4));
        assertEquals(
                new BigDecimal("9.50"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) commArray2_0.get(1).get("comItem5"), 2));
        /* ---- End Array 2, item 1*/

        assertEquals(3, commGroup1_0.get("comItem6"));

        assertEquals(2, commArray1.get(0).get("comItem7"));
        /* -- End Array 1, item 0*/

        /* -- Start Array 1, item 1*/
        assertEquals(19, commArray1.get(1).get("comItem2"));
        GenericRecord commGroup1_1 = (GenericRecord) commArray1.get(1).get(
                "comGroup1");
        assertEquals(12, commGroup1_1.get("comItem3"));
        GenericArray < GenericRecord > commArray2_1 = (GenericArray < GenericRecord >) commGroup1_1
                .get("comArray2");
        /* ---- Start Array 2, item 0*/
        assertEquals("B", commArray2_1.get(0).get("comItem4"));
        GenericArray < String > commArray3_1_0 = (GenericArray < String >) commArray2_1
                .get(0).get("comArray3");
        assertEquals("D", commArray3_1_0.get(0));
        assertEquals("B", commArray3_1_0.get(1));
        assertEquals("A", commArray3_1_0.get(2));
        assertEquals("E", commArray3_1_0.get(3));
        assertEquals("D", commArray3_1_0.get(4));
        assertEquals(
                new BigDecimal("38.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) commArray2_1.get(0).get("comItem5"), 2));
        /* ---- End Array 2, item 0*/

        /* ---- Start Array 2, item 1*/
        assertEquals("A", commArray2_1.get(1).get("comItem4"));
        GenericArray < String > commArray3_1_1 = (GenericArray < String >) commArray2_1
                .get(1).get("comArray3");
        assertEquals("E", commArray3_1_1.get(0));
        assertEquals("B", commArray3_1_1.get(1));
        assertEquals("B", commArray3_1_1.get(2));
        assertEquals("D", commArray3_1_1.get(3));
        assertEquals("A", commArray3_1_1.get(4));
        assertEquals(
                new BigDecimal("19.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) commArray2_1.get(1).get("comItem5"), 2));
        /* ---- End Array 2, item 1*/

        assertEquals(6, commGroup1_1.get("comItem6"));

        assertEquals(5, commArray1.get(1).get("comItem7"));
        /* -- End Array 1, item 1*/

        /* -- Start Array 1, item 2*/
        assertEquals(28, commArray1.get(2).get("comItem2"));
        GenericRecord commGroup1_2 = (GenericRecord) commArray1.get(2).get(
                "comGroup1");
        assertEquals(19, commGroup1_2.get("comItem3"));
        GenericArray < GenericRecord > commArray2_2 = (GenericArray < GenericRecord >) commGroup1_2
                .get("comArray2");
        /* ---- Start Array 2, item 0*/
        assertEquals("A", commArray2_2.get(0).get("comItem4"));
        GenericArray < String > commArray3_2_0 = (GenericArray < String >) commArray2_2
                .get(0).get("comArray3");
        assertEquals("E", commArray3_2_0.get(0));
        assertEquals("B", commArray3_2_0.get(1));
        assertEquals("E", commArray3_2_0.get(2));
        assertEquals("A", commArray3_2_0.get(3));
        assertEquals("C", commArray3_2_0.get(4));
        assertEquals(
                new BigDecimal("57.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) commArray2_2.get(0).get("comItem5"), 2));
        /* ---- End Array 2, item 0*/

        /* ---- Start Array 2, item 1*/
        assertEquals("D", commArray2_2.get(1).get("comItem4"));
        GenericArray < String > commArray3_2_1 = (GenericArray < String >) commArray2_2
                .get(1).get("comArray3");
        assertEquals("B", commArray3_2_1.get(0));
        assertEquals("C", commArray3_2_1.get(1));
        assertEquals("C", commArray3_2_1.get(2));
        assertEquals("C", commArray3_2_1.get(3));
        assertEquals("B", commArray3_2_1.get(4));
        assertEquals(
                new BigDecimal("28.50"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) commArray2_2.get(1).get("comItem5"), 2));
        /* ---- End Array 2, item 1*/

        assertEquals(9, commGroup1_2.get("comItem6"));

        assertEquals(8, commArray1.get(2).get("comItem7"));
        /* -- End Array 1, item 2*/

        assertEquals(
                new BigDecimal("237.50"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) genericRecord.get("comItem8"), 2));


        File file = avroSaveGeneric("stru04", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.stru04.Stru04Record.class,
                file);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCob2AvroAlltypes() throws Exception {

        GenericRecord genericRecord = toGenericRecord("alltypes",
                "c1c2c3c4"
                        + "01020000"
                        + "fc5c"
                        + "000f"
                        + "0001343a"
                        + "000001c4"
                        + "0000000000004532456d"
                        + "0000000000007800056f"
                        + "0000000000000000087554907654321c"
                        + "0000000000000000000564678008321f"
                        + "000007545f"
                        + "45543ae9"
                        + "361677a4590fab60"
                        + "c1c2c3c4"
                        + "c1c2c3c4"
                        + "40404040"
                        + "40404040"
                        + "fc5c"
                        + "fc5c"
                        + "000f"
                        + "000f"
                        + "0001343a"
                        + "0001343a"
                        + "000001c4"
                        + "000001c4"
                        + "0000000000004532456d"
                        + "0000000000004532456d"
                        + "0000000000007800056f"
                        + "0000000000007800056f"
                        + "0000000000000000087554907654321c"
                        + "0000000000000000087554907654321c"
                        + "0000000000000000000564678008321f"
                        + "0000000000000000000564678008321f"
                        + "000007545f"
                        + "000007545f"
                        + "45543ae9"
                        + "45543ae9"
                        + "361677a4590fab60"
                        + "361677a4590fab60",
                new legstar.avro.test.beans.alltypes.bind.AlltypesRecordBinding());

        assertEquals("ABCD", genericRecord.get("sString"));
        assertEquals("01020000", HostData.toHexString(((GenericFixed) genericRecord.get("sBinary")).bytes()));
        assertEquals(-932, genericRecord.get("sShort"));
        assertEquals(15, genericRecord.get("sUshort"));
        assertEquals(78906, genericRecord.get("sInt"));
        assertEquals(452l, genericRecord.get("sUint"));
        assertEquals(-4532456l, genericRecord.get("sLong"));
        assertEquals(7800056l, genericRecord.get("sUlong"));
        assertEquals(87554907654321l, genericRecord.get("sXlong"));
        assertEquals(564678008321l, genericRecord.get("sUxlong"));
        assertEquals(new BigDecimal("75.45"), DecimalUtils.toBigDecimal((ByteBuffer) genericRecord.get("sDec"), 2));
        assertEquals(.3450065677999998E+06f, genericRecord.get("sFloat"));
        assertEquals(.7982006699999985E-13d, genericRecord.get("sDouble"));
        
        assertEquals("[ABCD, ABCD]", genericRecord.get("aString").toString());
        assertEquals("[, ]", genericRecord.get("aBinary").toString());
        assertEquals("[-932, -932]", genericRecord.get("aShort").toString());
        assertEquals("[15, 15]", genericRecord.get("aUshort").toString());
        assertEquals("[78906, 78906]", genericRecord.get("aInt").toString());
        assertEquals("[452, 452]", genericRecord.get("aUint").toString());
        assertEquals("[-4532456, -4532456]", genericRecord.get("aLong").toString());
        assertEquals("[7800056, 7800056]", genericRecord.get("aUlong").toString());
        assertEquals("[87554907654321, 87554907654321]", genericRecord.get("aXlong").toString());
        assertEquals("[564678008321, 564678008321]", genericRecord.get("aUxlong").toString());
        assertEquals(new BigDecimal("75.45"), DecimalUtils.toBigDecimal(((GenericArray<ByteBuffer>) genericRecord.get("aDec")).get(0), 2));
        assertEquals(new BigDecimal("75.45"), DecimalUtils.toBigDecimal(((GenericArray<ByteBuffer>) genericRecord.get("aDec")).get(1), 2));
        assertEquals("[345006.56, 345006.56]", genericRecord.get("aFloat").toString());
        assertEquals("[7.982006699999985E-14, 7.982006699999985E-14]", genericRecord.get("aDouble").toString());
        
        File file = avroSaveGeneric("alltypes", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.alltypes.AlltypesRecord.class,
                file);

    }

    @SuppressWarnings("unchecked")
    @Test
    public void testCob2AvroCusdat() throws Exception {

        GenericRecord genericRecord = toGenericRecord("cusdat",
                "F0F0F0F0F0F1D1D6C8D540E2D4C9E3C840404040404040404040C3C1D4C2D9C9C4C7C540E4D5C9E5C5D9E2C9E3E8F4F4F0F1F2F5F6F500000002F1F061F0F461F1F1000000000023556C5C5C5C5C5C5C5C5C5CF1F061F0F461F1F1000000000023556C5C5C5C5C5C5C5C5C5C",
                new legstar.avro.test.beans.cusdat.bind.CustomerDataBinding());

        assertEquals(1l, genericRecord.get("customerId"));
        GenericRecord personalData = (GenericRecord) genericRecord.get("personalData");
        assertEquals("JOHN SMITH", personalData.get("customerName"));
        assertEquals("CAMBRIDGE UNIVERSITY", personalData.get("customerAddress"));
        assertEquals("44012565", personalData.get("customerPhone"));

        GenericRecord transactions = (GenericRecord) genericRecord.get("transactions");
        assertEquals(2l, transactions.get("transactionNbr"));
        GenericArray < GenericRecord > transaction = (GenericArray < GenericRecord >) transactions
                .get("transaction");
        GenericRecord transaction_0 = transaction.get(0);
        assertEquals("10/04/11", transaction_0.get("transactionDateChoice"));
        assertEquals(
                new BigDecimal("235.56"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) transaction_0.get("transactionAmount"), 2));
        assertEquals("*********", transaction_0.get("transactionComment"));
        GenericRecord transaction_1 = transaction.get(1);
        assertEquals("10/04/11", transaction_1.get("transactionDateChoice"));
        assertEquals(
                new BigDecimal("235.56"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) transaction_1.get("transactionAmount"), 2));
        assertEquals("*********", transaction_1.get("transactionComment"));

        File file = avroSaveGeneric("cusdat", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.cusdat.CustomerData.class,
                file);

    }

    @Test
    public void testCob2AvroRdef01Default() throws Exception {
        
        GenericRecord genericRecord = toGenericRecord("rdef01",
                "0000D5C1D4C5F0F0F0F0F0F50000D5C1D4C5F0F0F0F0F2F1",
                new legstar.avro.test.beans.rdef01.bind.Rdef01RecordBinding());

        assertEquals(0, genericRecord.get("comSelect"));
        GenericRecord comDetail1 = (GenericRecord) genericRecord.get("comDetail1Choice");
        assertEquals("NAME000005", comDetail1.get("comName"));

        File file = avroSaveGeneric("rdef01", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.rdef01.Rdef01Record.class,
                file);

    }

    @Test
    public void testCob2AvroRdef01Strategy() throws Exception {
        
        Rdef01RecordBinding rdef01RecordBinding = new Rdef01RecordBinding();
        rdef01RecordBinding._comDetail1Choice.setUnmarshalChoiceStrategy(new ICobolUnmarshalChoiceStrategy() {

            public ICobolBinding choose(ICobolChoiceBinding choice,
                    Hashtable < String, Object > variablesMap,
                    CobolElementVisitor visitor) throws HostException {
                Rdef01Record valueObject = (Rdef01Record) choice.getParentValueObject();
                int index = valueObject.getComSelect();
                switch (index) {
                case 0:
                    return choice.getAlternativeByName("ComDetail1");
                case 1:
                    return choice.getAlternativeByName("ComDetail2");
                case -1:
                    /* An exemple of how to signal an exception. */
                    throw (new HostException("Unable to select an alternative"));
                default:
                    /* Null will let the default choice strategy apply. */
                    return null;
                }
            }
            
        });

        GenericRecord genericRecord = toGenericRecord("rdef01",
                "00010250000F40404040404000010260000F404040404040",
                rdef01RecordBinding);

        assertEquals(1, genericRecord.get("comSelect"));
        GenericRecord comDetail1 = (GenericRecord) genericRecord.get("comDetail1Choice");
        assertEquals(
                new BigDecimal("2500.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) comDetail1.get("comAmount"), 2));

        File file = avroSaveGeneric("rdef01", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.rdef01.Rdef01Record.class,
                file);

    }

    @Test
    public void testCob2AvroRdef02Default() throws Exception {
        
        GenericRecord genericRecord = toGenericRecord("rdef02",
                "C1C2C3D1D2D30000D5C1D4C5F0F0F0F0F0F50260000F",
                new legstar.avro.test.beans.rdef02.bind.Rdef02RecordBinding());

        GenericRecord rdef02Key = (GenericRecord) genericRecord.get("rdef02Key");
        assertEquals("ABCJKL", rdef02Key.get("rdef02Item1Choice"));

        assertEquals(0, rdef02Key.get("comSelect"));
        GenericRecord comDetail1 = (GenericRecord) genericRecord.get("comDetail1Choice");
        assertEquals("NAME000005", comDetail1.get("comName"));
        assertEquals(
                new BigDecimal("2600.00"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) genericRecord.get("comItem3"), 2));

        File file = avroSaveGeneric("rdef02", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.rdef02.Rdef02Record.class,
                file);

    }

    @Test
    public void testCob2AvroRdef02DefaultAlt() throws Exception {
        
        GenericRecord genericRecord = toGenericRecord("rdef02",
                "00001361588C0000D5C1D4C5F0F0F0F0F0F50261588F",
                new legstar.avro.test.beans.rdef02.bind.Rdef02RecordBinding());

        GenericRecord rdef02Key = (GenericRecord) genericRecord.get("rdef02Key");
        assertEquals(1361588l, rdef02Key.get("rdef02Item1Choice"));

        assertEquals(0, rdef02Key.get("comSelect"));
        GenericRecord comDetail1 = (GenericRecord) genericRecord.get("comDetail1Choice");
        assertEquals("NAME000005", comDetail1.get("comName"));
        assertEquals(
                new BigDecimal("2615.88"),
                DecimalUtils.toBigDecimal(
                        (ByteBuffer) genericRecord.get("comItem3"), 2));

        File file = avroSaveGeneric("rdef02", genericRecord);
        avroReadSpecific(legstar.avro.test.specific.rdef02.Rdef02Record.class,
                file);

    }

   private GenericRecord toGenericRecord(String schemaName,
            String hostDataHex, ICobolComplexBinding ce) throws Exception {
        Schema schema = new Schema.Parser().parse(new File("target/gen/avsc/"
                + schemaName + ".avsc"));
        GenericRecord genericRecord = new GenericData.Record(schema);
        Cob2AvroUnmarshalVisitor visitor = new Cob2AvroUnmarshalVisitor(
                HostData.toByteArray(hostDataHex), genericRecord);
        visitor.visit(ce);
        return genericRecord;
    }

    private File avroSaveGeneric(String schemaName, GenericRecord genericRecord)
            throws IOException {
        File file = new File(avroFiles, schemaName + ".avro");
        DatumWriter < GenericRecord > datumWriter = new GenericDatumWriter < GenericRecord >(
                genericRecord.getSchema());
        DataFileWriter < GenericRecord > dataFileWriter = new DataFileWriter < GenericRecord >(
                datumWriter);
        dataFileWriter.create(genericRecord.getSchema(), file);
        dataFileWriter.append(genericRecord);
        dataFileWriter.close();
        return file;
    }

    private <T> void avroReadSpecific(Class < T > clazz, File file)
            throws IOException {
        DatumReader < T > datumReader = new SpecificDatumReader < T >(clazz);
        DataFileReader < T > dataFileReader = new DataFileReader < T >(file,
                datumReader);
        T specificRecord = null;
        while (dataFileReader.hasNext()) {
            specificRecord = dataFileReader.next(specificRecord);
            System.out.println(specificRecord);
        }
        dataFileReader.close();
    }

}
