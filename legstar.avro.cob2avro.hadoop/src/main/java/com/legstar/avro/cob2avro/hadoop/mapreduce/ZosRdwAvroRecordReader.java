package com.legstar.avro.cob2avro.hadoop.mapreduce;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.legstar.avro.cob2avro.io.ZosVarRdwDatumReader;
import com.legstar.base.context.CobolContext;
import com.legstar.base.finder.CobolTypeFinder;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.base.visitor.FromCobolChoiceStrategy;

/**
 * A Hadoop RecordReader for a mainframe file with records prefixed by a Record
 * Descriptor Word (RDW).
 * <p/>
 * Mainframe records are returned as Avro keys.
 * 
 * @param <T>
 */
public class ZosRdwAvroRecordReader<T> extends
        RecordReader < AvroKey < T >, NullWritable > {

    /** The mainframe COBOL parameters (including code page) */
    private final CobolContext cobolContext;

    /** The mainframe record type */
    private final CobolComplexType cobolType;

    /** Provides the capability to match the start of a new record */
    private final CobolTypeFinder recordMatcher;

    /**
     * Custom redefines alternative selector. Only needed when the incoming
     * record has redefines and the default strategy is not good enough.
     */
    private final FromCobolChoiceStrategy customChoiceStrategy;

    /** The Avro schema for records converted off the input mainframe file. */
    private final Schema schema;

    /** The current record from the Avro container file being read. */
    private T currentRecord;

    // Start and end positions of current split
    private long start = 0;
    private long end = 0;

    // Incoming file holding mainframe data
    private FSDataInputStream filein;

    // Avro datum reader for an rdw z/OS stream
    private ZosVarRdwDatumReader < T > datumReader;

    /**
     * Constructor.
     * 
     * @param cobolContext mainframe COBOL parameters
     * @param cobolType mainframe record type
     * @param customChoiceStrategy custom redefines alternative selector
     * @param recordMatcher provides the capability to match the start of a new
     *            record
     * @param schema Avro schema for records converted off the input mainframe
     *            file
     */
    protected ZosRdwAvroRecordReader(CobolContext cobolContext,
            CobolComplexType cobolType,
            FromCobolChoiceStrategy customChoiceStrategy,
            CobolTypeFinder recordMatcher, Schema schema) {
        this.cobolContext = cobolContext;
        this.cobolType = cobolType;
        this.customChoiceStrategy = customChoiceStrategy;
        this.schema = schema;
        this.recordMatcher = recordMatcher;
        this.currentRecord = null;
    }

    /** {@inheritDoc} */
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        if (!(split instanceof FileSplit)) {
            throw new IllegalArgumentException(
                    "Only compatible with FileSplits.");
        }

        FileSplit fileSplit = (FileSplit) split;
        final Path file = fileSplit.getPath();
        start = fileSplit.getStart();
        end = start + fileSplit.getLength();

        Configuration jobConf = context.getConfiguration();
        FileSystem fs = file.getFileSystem(jobConf);
        filein = fs.open(fileSplit.getPath());

        // The reader must be able to read past the last byte of a split if a
        // record spans this split and the next. This is why the length passed
        // to the reader is from split start to end of file (not end of split)
        long readLen = fs.getFileStatus(fileSplit.getPath()).getLen()
                - (start > 0 ? start - 1 : 0);
        datumReader = new ZosVarRdwDatumReader < T >(filein, readLen,
                cobolContext, cobolType, customChoiceStrategy, schema);

        if (start > 0) {
            // This is a subsequent split
            --start;

            // Position at the start of a record
            filein.seek(start);
            datumReader.seekRecordStart(recordMatcher);
        }

    }

    public boolean nextKeyValue() throws IOException, InterruptedException {
        // If we have already read all bytes from this split, time to stop
        if (datumReader.getBytesRead() >= (end - start)) {
            return false;
        }

        if (datumReader.hasNext()) {
            currentRecord = datumReader.next();
            return true;
        } else {
            return false;
        }
    }

    public AvroKey < T > getCurrentKey() throws IOException,
            InterruptedException {
        return new AvroKey < T >(currentRecord);
    }

    public NullWritable getCurrentValue() throws IOException,
            InterruptedException {
        return NullWritable.get();
    }

    public float getProgress() throws IOException, InterruptedException {
        return (start == end) ? 0.0f : Math.min(1.0f,
                datumReader.getBytesRead() / (float) (end - start));
    }

    public void close() throws IOException {
        if (null != datumReader) {
            try {
                datumReader.close();
            } finally {
                datumReader = null;
            }
        }
    }

}
