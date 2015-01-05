package com.legstar.avro.cob2avro.hadoop.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;

import com.legstar.base.context.CobolContext;
import com.legstar.base.finder.CobolTypeFinder;
import com.legstar.base.type.composite.CobolComplexType;
import com.legstar.base.visitor.FromCobolChoiceStrategy;

/**
 * Configuration helper tool for Jobs using Cob2Avro Readers/Writers.
 * <p/>
 * Similar to {@link org.apache.avro.mapreduce.AvroJob}
 *
 */
public class Cob2AvroJob {

    /** Configuration key for the input key mainframe COBOL parameters. */
    private static final String CONF_INPUT_KEY_COBOL_CONTEXT = "cob2avro.cobol.context.input.key";

    /** Configuration key for the input key mainframe record type. */
    private static final String CONF_INPUT_KEY_RECORD_TYPE = "cob2avro.record.type.input.key";

    /** Configuration key for the input key record matcher class. */
    private static final String CONF_INPUT_RECORD_MATCHER_CLASS = "cob2avro.matcher.class.input.key";

    /** Configuration key for the input key choice strategy class. */
    private static final String CONF_INPUT_RECORD_CHOICE_STRATEGY_CLASS = "cob2avro.choice.strategy.class.input.key";

    /** Disable the constructor for this utility class. */
    private Cob2AvroJob() {}

    /**
     * Sets the job input key mainframe COBOL parameters (including code page).
     *
     * @param job The job to configure.
     * @param cobolContext The input key mainframe COBOL parameters.
     */
    public static void setInputKeyCobolContext(Job job, Class<? extends CobolContext> cobolContext) {
      job.getConfiguration().setClass(CONF_INPUT_KEY_COBOL_CONTEXT, cobolContext, CobolContext.class);
    }

    /**
     * Gets the job input key mainframe COBOL parameters.
     *
     * @param conf The job configuration.
     * @return The job input key mainframe COBOL parameters, or null if not set.
     */
    public static Class<? extends CobolContext> getInputKeyCobolContext(Configuration conf) {
      return conf.getClass(CONF_INPUT_KEY_COBOL_CONTEXT, null, CobolContext.class);
    }

    /**
     * Sets the job input key mainframe record type.
     *
     * @param job The job to configure.
     * @param cobolType The input key mainframe record type.
     */
    public static void setInputKeyRecordType(Job job, Class<? extends CobolComplexType> cobolType) {
      job.getConfiguration().setClass(CONF_INPUT_KEY_RECORD_TYPE, cobolType, CobolComplexType.class);
    }

    /**
     * Gets the job input key mainframe record type.
     *
     * @param conf The job configuration.
     * @return The job input key mainframe record type, or null if not set.
     */
    public static Class<? extends CobolComplexType> getInputKeyRecordType(Configuration conf) {
      return conf.getClass(CONF_INPUT_KEY_RECORD_TYPE, null, CobolComplexType.class);
    }

    /**
     * Sets the job input record matcher class.
     *
     * @param job The job to configure.
     * @param matcherClass The input record matcher class.
     */
    public static void setInputRecordMatcher(Job job, Class<? extends CobolTypeFinder> matcherClass) {
      job.getConfiguration().setClass(CONF_INPUT_RECORD_MATCHER_CLASS, matcherClass, CobolTypeFinder.class);
    }

    /**
     * Gets the job input record matcher class.
     *
     * @param conf The job configuration.
     * @return The job input record matcher class, or null if not set.
     */
    public static Class<? extends CobolTypeFinder> getInputRecordMatcher(Configuration conf) {
      return conf.getClass(CONF_INPUT_RECORD_MATCHER_CLASS, null, CobolTypeFinder.class);
    }

    /**
     * Sets the job input record choice strategy class.
     *
     * @param job The job to configure.
     * @param choiceStrategyClass The input record choice strategy class.
     */
    public static void setInputChoiceStrategy(Job job, Class<? extends FromCobolChoiceStrategy> choiceStrategyClass) {
      job.getConfiguration().setClass(CONF_INPUT_RECORD_CHOICE_STRATEGY_CLASS, choiceStrategyClass, FromCobolChoiceStrategy.class);
    }

    /**
     * Gets the job input record choice strategy class.
     *
     * @param conf The job configuration.
     * @return The job input record choice strategy class, or null if not set.
     */
    public static Class<? extends FromCobolChoiceStrategy> getInputChoiceStrategy(Configuration conf) {
      return conf.getClass(CONF_INPUT_RECORD_CHOICE_STRATEGY_CLASS, null, FromCobolChoiceStrategy.class);
    }


}
