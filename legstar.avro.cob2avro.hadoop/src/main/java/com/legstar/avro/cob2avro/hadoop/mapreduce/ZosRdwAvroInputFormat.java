package com.legstar.avro.cob2avro.hadoop.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.legstar.avro.cob2avro.ZosRecordMatcher;
import com.legstar.coxb.ICobolComplexBinding;

/**
 * Reads a mainframe file into Avro keys.
 * <p/>
 * The mainframe binary file resides on HDFS and has potentially been split on
 * boundaries that do not match a mainframe record boundary. As a result a
 * mainframe record might end up spanning 2 splits.
 * <p/>
 * See {@link ZosRdwAvroRecordReader} for logic that resolves the multi-split
 * spanning issue.
 * <p/>
 * This InputFormat must be configured with
 * <ul>
 * <li>An Avro schema for the input key</li>
 * <li>A {@link ICobolComplexBinding}</li> class that binds the COBOL copybook
 * describing the mainframe record to java
 * <li>A {@link ZosRecordMatcher}</li> class that detects the start of a
 * mainframe record
 * </ul>
 * 
 * @param <T> the Avro specific class
 */
public class ZosRdwAvroInputFormat<T> extends
        FileInputFormat < AvroKey < T >, NullWritable > {

    private static final Logger LOG = LoggerFactory.getLogger(ZosRdwAvroInputFormat.class);

    public RecordReader < AvroKey < T >, NullWritable > createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        try {
            Schema readerSchema = AvroJob.getInputKeySchema(context
                    .getConfiguration());
            Class < ? extends ICobolComplexBinding > bindingClass = Cob2AvroJob
                    .getInputKeyBindingClass(context.getConfiguration());
            Class < ? extends ZosRecordMatcher > matcherClass = Cob2AvroJob
                    .getInputRecordMatcherClass(context.getConfiguration());

            if (!isValid(readerSchema, bindingClass, matcherClass)) {
                throw new IOException("Invalid configuration");
            }
            
            return new ZosRdwAvroRecordReader < T >(readerSchema,
                    bindingClass.newInstance(), matcherClass.newInstance());

        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
    }
    
    private boolean isValid(Schema readerSchema,
            Class < ? extends ICobolComplexBinding > bindingClass,
            Class < ? extends ZosRecordMatcher > matcherClass) {

        boolean valid = true;
        if (null == readerSchema) {
            LOG.error("Reader schema was not set. Use AvroJob.setInputKeySchema().");
            valid = false;
        }
        if (null == bindingClass) {
            LOG.error("Reader COBOL binding class was not set. Use Cob2AvroJob.setInputKeyBindingClass().");
            valid = false;
        }
        if (null == bindingClass) {
            LOG.error("Reader record matcher class was not set. Use Cob2AvroJob.setInputRecordMatcherClass().");
            valid = false;
        }
        return valid;

    }

}
