package com.legstar.avro.generator;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exposes the {@link Cob2AvroTransGenerator} utility as a command line tool.
 * 
 */
public class Cob2AvroTransMain {

    /** Options that can be setup. */
    private static final String OPTION_INPUT = "input";

    private static final String OPTION_OUTPUT = "output";

    private static final String OPTION_CLASSPATH = "classpath";

    private static final String OPTION_CONFIG = "config";

    private static final String OPTION_HELP = "help";

    private static final String OPTION_VERSION = "version";

    private static Logger log = LoggerFactory
            .getLogger(Cob2AvroTransMain.class);

    /** The defaults. */
    private static final String DEFAULT_INPUT_FOLDER_PATH = "cobol";

    private static final String DEFAULT_OUTPUT_FOLDER_PATH = "target/gen";

    /**
     * A COBOL copybook file or a folder containing COBOL copybooks. Defaults to
     * cobol relative folder.
     */
    private File cobolInput;

    /**
     * A folder containing generated artifacts. Defaults to target relative
     * folder.
     */
    private File output;

    /**
     * a java class path to use by compiler to locate dependencies.
     */
    private String classpath;

    /**
     * An optional configuration file for the generator.
     */
    private File configFile;

    /**
     * @param args translator options. Provides help if no arguments passed.
     */
    public static void main(final String[] args) {
        Cob2AvroTransMain main = new Cob2AvroTransMain();
        boolean success = main.execute(args);
        if (!success) {
            throw new RuntimeException("Cob2AvroTransMain failure");         
        }
        // System.exit(main.execute(args) ? 0 : 12);
    }

    /**
     * Process command line options and run translator.
     * <p/>
     * If no options are passed, prints the help. Help is also printed if the
     * command line options are invalid.
     * 
     * @param args translator options
     * @return true if execution succeeded
     */
    public boolean execute(final String[] args) {
        Options options = createOptions();
        boolean rc = true;
        if (collectOptions(options, args)) {
            setDefaults();
            Cob2AvroTransGenerator gen = new Cob2AvroTransGenerator();
            if (cobolInput.isDirectory()) {
                rc &= gen.generate(FileUtils.listFiles(cobolInput, null, true),
                        output, configFile, classpath);
            } else {
                rc = gen.generate(cobolInput, output, configFile, classpath);
            }
        }
        return rc;
    }

    /**
     * @return the command line options
     */
    private Options createOptions() {
        Options options = new Options();

        Option version = new Option("v", OPTION_VERSION, false,
                "print the version information and exit");
        options.addOption(version);

        Option help = new Option("h", OPTION_HELP, false,
                "print the options available");
        options.addOption(help);

        Option input = new Option("i", OPTION_INPUT, true,
                "file or folder holding the COBOL copybooks to translate."
                        + " Name is relative or absolute");
        options.addOption(input);

        Option output = new Option("o", OPTION_OUTPUT, true,
                "folder receiving the generated artifacts");
        options.addOption(output);

        Option classpath = new Option("cp", OPTION_CLASSPATH, true,
                "java class path to use by compiler to locate dependencies");
        options.addOption(classpath);

        Option config = new Option("c", OPTION_CONFIG, true,
                "optional configuration file");
        options.addOption(config);

        return options;
    }

    /**
     * Take arguments received on the command line and setup corresponding
     * options.
     * <p/>
     * No arguments is valid. It means use the defaults.
     * 
     * @param options the expected options
     * @param args the actual arguments received on the command line
     * @return true if arguments were valid
     */
    private boolean collectOptions(final Options options, final String[] args) {
        try {
            if (args != null && args.length > 0) {
                CommandLineParser parser = new PosixParser();
                CommandLine line = parser.parse(options, args);
                return processLine(line, options);
            }
            return true;
        } catch (Exception e) {
            log.error("Unable to parse options", e);
            return false;
        }
    }

    /**
     * Process the command line options selected.
     * 
     * @param line the parsed command line
     * @param options available
     * @return false if processing needs to stop, true if its ok to continue
     * @throws Exception if line cannot be processed
     */
    private boolean processLine(final CommandLine line, final Options options)
            throws Exception {
        if (line.hasOption(OPTION_VERSION)) {
            log.info(getVersion(true));
            return false;
        }
        if (line.hasOption(OPTION_HELP)) {
            produceHelp(options);
            return false;
        }
        if (line.hasOption(OPTION_INPUT)) {
            setCobolInput(line.getOptionValue(OPTION_INPUT).trim());
        }
        if (line.hasOption(OPTION_OUTPUT)) {
            setOutput(line.getOptionValue(OPTION_OUTPUT).trim());
        }
        if (line.hasOption(OPTION_CLASSPATH)) {
            setClasspath(line.getOptionValue(OPTION_CLASSPATH).trim());
        }
        if (line.hasOption(OPTION_CONFIG)) {
            setConfigFile(line.getOptionValue(OPTION_CONFIG).trim());
        }

        return true;
    }

    /**
     * Retrieve the current version.
     * 
     * @parm verbose when true will also return the build date
     * @return the version number and build date
     */
    private String getVersion(boolean verbose) {
        try {
            InputStream stream = getClass().getResourceAsStream("/version.properties");
            Properties props = new Properties();
            props.load(stream);
            if (verbose) {
                return String.format("Version=%s, build date=%s",
                        props.getProperty("version"),
                        props.getProperty("buildDate"));
            } else {
                return props.getProperty("version");
            }
        } catch (IOException e) {
            log.error("Unable to retrieve version", e);
            return "unknown";
        }
    }

    /**
     * @param options options available
     * @throws Exception if help cannot be produced
     */
    private void produceHelp(final Options options) throws Exception {
        HelpFormatter formatter = new HelpFormatter();
        String version = getVersion(false);
        formatter.printHelp("java -jar cascading.legstar.translator-" + version
                + "-exe.jar followed by:", options);
    }

    /**
     * Make sure mandatory parameters have default values.
     */
    private void setDefaults() {
        if (cobolInput == null) {
            setCobolInput(DEFAULT_INPUT_FOLDER_PATH);
        }
        if (output == null) {
            setOutput(DEFAULT_OUTPUT_FOLDER_PATH);
        }

    }

    /**
     * Check the input parameter and keep it only if it is valid.
     * 
     * @param cobolInputPath a file or folder name (relative or absolute)
     */
    public void setCobolInput(final String cobolInputPath) {
        if (cobolInputPath == null) {
            throw (new IllegalArgumentException(
                    "You must provide a source folder or file"));
        }
        File cobolInput = new File(cobolInputPath);
        if (cobolInput.exists()) {
            if (cobolInput.isDirectory() && cobolInput.list().length == 0) {
                throw new IllegalArgumentException("Folder '" + cobolInputPath
                        + "' is empty");
            }
        } else {
            throw new IllegalArgumentException("Input file or folder '"
                    + cobolInputPath + "' not found");
        }
        this.cobolInput = cobolInput;
    }

    /**
     * Check the output parameter and keep it only if it is valid.
     * 
     * @param output a file or folder name (relative or absolute)
     */
    public void setOutput(final String output) {
        if (output == null) {
            throw (new IllegalArgumentException(
                    "You must provide a target folder or file"));
        }
        this.output = new File(output);
    }

    /**
     * @param classpath the java class path to use by compiler to locate dependencies to set
     */
    public void setClasspath(final String classpath) {
        this.classpath = classpath;
    }

    /**
     * @param classpath the optional configuration file path
     */
    public void setConfigFile(final String config) {
        this.configFile = config == null ? null : new File(config);
    }
}
