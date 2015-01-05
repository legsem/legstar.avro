package com.legstar.avro.cob2avro.hadoop.mapreduce;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import legstar.test.avro.custdat.CobolCustomerData;
import legstar.test.avro.custdat.CustomerData;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.legstar.avro.cob2avro.hadoop.mapreduce.Cob2AvroJob;
import com.legstar.avro.cob2avro.hadoop.mapreduce.ZosRdwAvroInputFormat;
import com.legstar.base.context.EbcdicCobolContext;

public class ZosRdwAvroRecordReaderTest {

    private static final String OUTPUT_LOCAL_FILE = "custdat.csv";

    private static final String OUTPUT_HADOOP_DATA_PATH = "/user/legstar.avro/out";

    private Configuration conf;
    private MiniDFSCluster hdfsCluster;
    private FileSystem fs;
    private Path srcFilePath = new Path("src/test/data/ZOS.FCUSTDAT.RDW.bin");
    private Path datFilePath = new Path(
            "/user/legstar.avro/ZOS.FCUSTDAT.RDW.bin");

    private static final File OUTPUT_LOCAL_FOLDER = new File("target/test/csv");

    @Rule
    public TestName testName = new TestName();

    @Before
    public void setUp() throws Exception {
        File baseDir = new File("./target/hdfs/" + testName).getAbsoluteFile();
        FileUtil.fullyDelete(baseDir);
        FileUtils.forceMkdir(OUTPUT_LOCAL_FOLDER);
        FileUtils.cleanDirectory(OUTPUT_LOCAL_FOLDER);

        conf = new Configuration();
        conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, baseDir.getAbsolutePath());

        // Force small splits
        conf.set("dfs.blocksize", "1048576"); // 2 splits (total bytes=1249075)

        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
        hdfsCluster = builder.build();
        String hdfsURI = "hdfs://localhost:" + hdfsCluster.getNameNodePort()
                + "/";
        System.out.println("Hadoop fs " + hdfsURI);

        // Copy the local mainframe file to hdfs (which creates splits)
        fs = FileSystem.get(conf);
        fs.mkdirs(new Path("/user/legstar.avro"));
        fs.copyFromLocalFile(srcFilePath, datFilePath);
    }

    @After
    public void tearDown() throws Exception {
        hdfsCluster.shutdown();
    }

    @Test
    public void testRecordCountPerCustomer() throws Exception {

        Path inPath = datFilePath;
        Path outPath = new Path(OUTPUT_HADOOP_DATA_PATH);

        Job job = Job.getInstance(conf);
        job.setJobName("recordsPerCustomer");

        FileInputFormat.addInputPath(job, inPath);
        job.setInputFormatClass(ZosRdwAvroInputFormat.class);
        Cob2AvroJob.setInputKeyCobolContext(job, EbcdicCobolContext.class);
        Cob2AvroJob.setInputKeyRecordType(job, CobolCustomerData.class);
        Cob2AvroJob.setInputRecordMatcher(job, CustdatZosRdwRecordMatcher.class);
        AvroJob.setInputKeySchema(job, CustomerData.getClassSchema());
        job.setMapperClass(MyMapper.class);

        FileOutputFormat.setOutputPath(job, outPath);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        job.waitForCompletion(true);

        // Get the CSV locally and check its content
        FileUtil.copyMerge(fs, new Path(OUTPUT_HADOOP_DATA_PATH),
                FileSystem.getLocal(conf),
                new Path(OUTPUT_LOCAL_FOLDER.getPath() + "/"
                        + OUTPUT_LOCAL_FILE), false, conf, null);

        List < String > lines = FileUtils.readLines(new File(
                OUTPUT_LOCAL_FOLDER, OUTPUT_LOCAL_FILE));
        assertEquals(25, lines.size());
        int count = 0;
        for (String line : lines) {
            String[] cols = line.split("\t");
            assertEquals(2, cols.length);
            count += Integer.parseInt(cols[1]);
        }
        assertEquals(10000, count);

    }

    public static class MyMapper extends
            Mapper < AvroKey < CustomerData >, NullWritable, Text, IntWritable > {

        public void map(AvroKey < CustomerData > key, NullWritable value,
                Context context) throws IOException, InterruptedException {
            CharSequence customerName = key.datum().getPersonalData()
                    .getCustomerName();
            context.write(new Text(customerName.toString()), new IntWritable(1));
        }
    }

    public static class MyReducer extends
            Reducer < Text, IntWritable, Text, IntWritable > {

        public void reduce(Text key, Iterable < IntWritable > values,
                Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}
