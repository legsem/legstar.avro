package com.legstar.avro.generator;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.codehaus.jackson.JsonNode;

/**
 * Temporary workaround for the lack of support for BigDecimal in Avro Specific Compiler.
 * <p/>
 * The record.vm template is customized to expose BigDecimal getters and setters.
 *
 */
public class CustomSpecificCompiler extends SpecificCompiler {

    private static final String TEMPLATES_PATH = "/com/legstar/avro/generator/specific/templates/java/classic/";

    public CustomSpecificCompiler(Schema schema) {
        super(schema);
        setTemplateDir(TEMPLATES_PATH);
    }

    /**
     * In the case of BigDecimals there is an internal java type (ByteBuffer)
     * and an external java type for getters/setters.
     * 
     * @param schema the field schema
     * @return the field java type
     */
    public String externalJavaType(Schema schema) {
        return isBigDecimal(schema) ? "java.math.BigDecimal" : super
                .javaType(schema);
    }

    /** Tests whether a field is to be externalized as a BigDecimal */
    public static boolean isBigDecimal(Schema schema) {
        if (Type.BYTES == schema.getType()) {
            JsonNode logicalTypeNode = schema.getJsonProp("logicalType");
            if (logicalTypeNode != null
                    && "decimal".equals(logicalTypeNode.asText())) {
                return true;
            }
        }
        return false;
    }
    
}
