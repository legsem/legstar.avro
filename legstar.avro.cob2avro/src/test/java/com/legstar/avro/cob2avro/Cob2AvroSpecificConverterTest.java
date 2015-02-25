package com.legstar.avro.cob2avro;

import static org.junit.Assert.*;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.avro.specific.SpecificRecord;
import org.junit.Before;
import org.junit.Test;

import com.legstar.base.converter.FromHostResult;
import com.legstar.base.type.CobolType;
import com.legstar.base.type.composite.CobolChoiceType;
import com.legstar.base.utils.HexUtils;
import com.legstar.base.visitor.FromCobolChoiceStrategy;

public class Cob2AvroSpecificConverterTest extends AbstractTest {

    private static final boolean CREATE_REFERENCE = false;

    @Before
    public void setUp() {
        setCreateReferences(CREATE_REFERENCE);
    }

    @Test
    public void testConvertFlat01() {
        Cob2AvroSpecificConverter converter = new Cob2AvroSpecificConverter.Builder()
                .cobolComplexType(new legstar.test.avro.flat01.CobolFlat01Record())
                .schema(getSchema("flat01"))
                .build();
        FromHostResult < SpecificRecord > result = converter.convert(
                        HexUtils.decodeHex("F0F0F1F0F4F3D5C1D4C5F0F0F0F0F4F3404040404040404040400215000F"));
        assertEquals(30, result.getBytesProcessed());
        check(avro2Json(result.getValue()), "result.json");
    }

    @Test
    public void testConvertRdef01Strategy() {
 
        FromCobolChoiceStrategy customChoiceStrategy = new FromCobolChoiceStrategy() {

            public CobolType choose(String choiceFieldName,
                    CobolChoiceType choiceType,
                    Map < String, Object > variables, byte[] hostData,
                    int start, int length) {
                int select = ((Number) variables.get("comSelect"))
                        .intValue();
                switch (select) {
                case 0:
                    return choiceType.getAlternatives().get(
                            "ComDetail1");
                case 1:
                    return choiceType.getAlternatives().get(
                            "ComDetail2");
                default:
                    return null;
                }
            }

            public Set < String > getVariableNames() {
                Set < String > variables = new HashSet < String >();
                variables.add("comSelect");
                return variables;
            }

        };
        
        Cob2AvroSpecificConverter converter = new Cob2AvroSpecificConverter.Builder()
                .cobolComplexType(new legstar.test.avro.rdef01.CobolRdef01Record())
                .schema(getSchema("rdef01"))
                .customChoiceStrategy(customChoiceStrategy)
                .build();
        FromHostResult < SpecificRecord > result = converter.convert(
                        HexUtils.decodeHex("00010250000F40404040404000010260000F404040404040"));
        assertEquals(6, result.getBytesProcessed());
        check(avro2Json(result.getValue()), "result.json");
    }


}
