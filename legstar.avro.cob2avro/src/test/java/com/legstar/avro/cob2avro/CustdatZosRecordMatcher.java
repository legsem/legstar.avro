package com.legstar.avro.cob2avro;

import legstar.test.avro.custdat.CobolCustomerData;

import com.legstar.base.context.EbcdicCobolContext;
import com.legstar.base.finder.CobolComplexTypeFinder;

/**
 * A matcher for the custdat mainframe file (version that does not start with an
 * RDW).
 * <p/>
 * The 58 first bytes of a custdat record are laid out like this:
 * <ul>
 * <li>First 6 characters are EBCDIC digits</li>
 * <li>Next 48 characters are EBCDIC text</li>
 * <li>Last 4 bytes are an integer with a value between 0 and 5</li>
 * </ul>
 * We consider these 58 characters form a unique signature.
 * 
 */
public class CustdatZosRecordMatcher extends CobolComplexTypeFinder {

    public CustdatZosRecordMatcher() {
        super(new EbcdicCobolContext(), new CobolCustomerData(), "transactionNbr");
    }
}
