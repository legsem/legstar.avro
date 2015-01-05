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

import com.legstar.base.context.CobolContext;
import com.legstar.base.finder.CobolTypeFinder;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.base.visitor.FromCobolChoiceStrategy;

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

    private static final Logger LOG = LoggerFactory
            .getLogger(ZosRdwAvroInputFormat.class);

    public RecordReader < AvroKey < T >, NullWritable > createRecordReader(
            InputSplit split, TaskAttemptContext context) throws IOException,
            InterruptedException {
        try {
            Class < ? extends CobolContext > cobolContextClass = Cob2AvroJob
                    .getInputKeyCobolContext(context.getConfiguration());
            Class < ? extends CobolComplexType > cobolTypeClass = Cob2AvroJob
                    .getInputKeyRecordType(context.getConfiguration());
            Class < ? extends CobolTypeFinder > matcherClass = Cob2AvroJob
                    .getInputRecordMatcher(context.getConfiguration());
            Class < ? extends FromCobolChoiceStrategy > choiceStrategyClass = Cob2AvroJob
                    .getInputChoiceStrategy(context.getConfiguration());
            Schema schema = AvroJob.getInputKeySchema(context
                    .getConfiguration());

            if (!isValid(cobolContextClass, cobolTypeClass, matcherClass,
                    choiceStrategyClass, schema)) {
                throw new IOException("Invalid configuration");
            }

            return new ZosRdwAvroRecordReader < T >(
                    cobolContextClass.newInstance(),
                    cobolTypeClass.newInstance(),
                    choiceStrategyClass == null ? null : choiceStrategyClass
                            .newInstance(), matcherClass.newInstance(), schema);

        } catch (InstantiationException e) {
            throw new IOException(e);
        } catch (IllegalAccessException e) {
            throw new IOException(e);
        }
    }

    private boolean isValid(Class < ? extends CobolContext > cobolContextClass,
            Class < ? extends CobolComplexType > cobolTypeClass,
            Class < ? extends CobolTypeFinder > matcherClass,
            Class < ? extends FromCobolChoiceStrategy > choiceStrategyClass,
            Schema schema) {

        boolean valid = true;
        if (null == cobolContextClass) {
            LOG.error("Mainframe COBOL parameters class was not set. Use Cob2AvroJob.setInputKeyCobolContext().");
            valid = false;
        }
        if (null == cobolTypeClass) {
            LOG.error("Mainframe record type class was not set. Use Cob2AvroJob.setInputKeyRecordType().");
            valid = false;
        }
        if (null == matcherClass) {
            LOG.error("Mainframe record matcher class was not set. Use Cob2AvroJob.setInputRecordMatcher().");
            valid = false;
        }
        if (null == schema) {
            LOG.error("Reader schema was not set. Use AvroJob.setInputKeySchema().");
            valid = false;
        }
        return valid;

    }

}
