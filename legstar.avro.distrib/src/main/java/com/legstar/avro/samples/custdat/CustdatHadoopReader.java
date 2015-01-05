package com.legstar.avro.samples.custdat;

import java.io.IOException;

import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.legstar.avro.cob2avro.hadoop.mapreduce.Cob2AvroJob;
import com.legstar.avro.cob2avro.hadoop.mapreduce.ZosRdwAvroInputFormat;
import com.legstar.base.context.EbcdicCobolContext;

public class CustdatHadoopReader extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CustdatHadoopReader(), args);
        System.exit(res);
    }
    
    public int run(String[] args) throws Exception {

        // Create configuration
        Configuration conf = this.getConf();

        // Create job
        Job job = Job.getInstance(conf);
        job.setJobName("recordsPerCustomer");
        job.setJarByClass(CustdatHadoopReader.class);

        // Setup MapReduce classes
        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        // Set only 1 reduce task
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        job.setInputFormatClass(ZosRdwAvroInputFormat.class);
        Cob2AvroJob.setInputKeyCobolContext(job, EbcdicCobolContext.class);
        Cob2AvroJob.setInputKeyRecordType(job, CobolCustomerData.class);
        Cob2AvroJob.setInputRecordMatcher(job, CustdatZosRdwRecordMatcher.class);
        AvroJob.setInputKeySchema(job, CustomerData.getClassSchema());
        job.setMapperClass(MyMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setReducerClass(MyReducer.class);

        // Execute job
        return job.waitForCompletion(true) ? 0 : 1;
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
