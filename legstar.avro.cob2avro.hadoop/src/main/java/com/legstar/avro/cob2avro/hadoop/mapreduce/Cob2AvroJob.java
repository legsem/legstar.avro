package com.legstar.avro.cob2avro.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.legstar.coxb.ICobolComplexBinding;
import com.legstar.avro.cob2avro.ZosRecordMatcher;

/**
 * Configuration helper tool for Jobs using Cob2Avro Readers/Writers.
 * <p/>
 * Similar to {@link org.apache.avro.mapreduce.AvroJob}
 *
 */
public class Cob2AvroJob {

    /** Configuration key for the input key binding class. */
    private static final String CONF_INPUT_KEY_BINDING_CLASS = "cob2avro.binding.class.input.key";

    /** Configuration key for the input record matcher class. */
    private static final String CONF_INPUT_RECORD_MATCHER_CLASS = "cob2avro.matcher.class.input.key";

    /** Disable the constructor for this utility class. */
    private Cob2AvroJob() {}

    /**
     * Sets the job input key binding class.
     *
     * @param job The job to configure.
     * @param bindingClass The input key binding class.
     */
    public static void setInputKeyBindingClass(Job job, Class<? extends ICobolComplexBinding> bindingClass) {
      job.getConfiguration().setClass(CONF_INPUT_KEY_BINDING_CLASS, bindingClass, ICobolComplexBinding.class);
    }

    /**
     * Gets the job input key binding class.
     *
     * @param conf The job configuration.
     * @return The job input key binding class, or null if not set.
     */
    public static Class<? extends ICobolComplexBinding> getInputKeyBindingClass(Configuration conf) {
      return conf.getClass(CONF_INPUT_KEY_BINDING_CLASS, null, ICobolComplexBinding.class);
    }

    /**
     * Sets the job input record matcher class.
     *
     * @param job The job to configure.
     * @param matcherClass The input record matcher class.
     */
    public static void setInputRecordMatcherClass(Job job, Class<? extends ZosRecordMatcher> matcherClass) {
      job.getConfiguration().setClass(CONF_INPUT_RECORD_MATCHER_CLASS, matcherClass, ZosRecordMatcher.class);
    }

    /**
     * Gets the job input record matcher class.
     *
     * @param conf The job configuration.
     * @return The job input record matcher class, or null if not set.
     */
    public static Class<? extends ZosRecordMatcher> getInputRecordMatcherClass(Configuration conf) {
      return conf.getClass(CONF_INPUT_RECORD_MATCHER_CLASS, null, ZosRecordMatcher.class);
    }


}
