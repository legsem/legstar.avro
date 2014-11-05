package com.legstar.avro.generator;

import java.util.Properties;

import com.legstar.coxb.cob2trans.Cob2TransModel;

public class Cob2AvroTransModel extends Cob2TransModel {

    /* ====================================================================== */
    /* Following are default field values. = */
    /* ====================================================================== */

    /** Default Java package name prefix for generated avro specific classes. */
    public static final String DEFAULT_AVRO_PACKAGE_PREFIX = "com.legstar.avro.specific";

    /* ====================================================================== */
    /* Following are key identifiers for this model persistence. = */
    /* ====================================================================== */

    /** Java package name prefix for generated avro specific classes. */
    public static final String AVRO_PACKAGE_PREFIX = "avroPackagePrefix";

    /* ====================================================================== */
    /* Following are this class fields that are persistent. = */
    /* ====================================================================== */

    /** Java package name prefix for generated avro specific classes. */
    private String avroPackagePrefix = DEFAULT_AVRO_PACKAGE_PREFIX;


    /**
     * Construct from a properties file.
     * 
     * @param props the property file
     */
    public Cob2AvroTransModel(final Properties props) {
        super(props);
        setAvroPackagePrefix(getString(props, AVRO_PACKAGE_PREFIX,
                DEFAULT_AVRO_PACKAGE_PREFIX));
        
    }

    public String getAvroPackagePrefix() {
        return avroPackagePrefix;
    }


    public void setAvroPackagePrefix(String avroPackagePrefix) {
        this.avroPackagePrefix = avroPackagePrefix;
    }


    /**
     * @return a properties file holding the values of this object fields
     */
    public Properties toProperties() {
        Properties props = super.toProperties();
        if (getAvroPackagePrefix() != null) {
            putString(props, AVRO_PACKAGE_PREFIX, getAvroPackagePrefix());
        }
        return props;
    }
}
