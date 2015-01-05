package com.legstar.avro.cob2avro;

import legstar.test.avro.custdat.CobolCustomerData;

import com.legstar.base.context.EbcdicCobolContext;
import com.legstar.base.finder.RdwCobolComplexTypeFinder;

/**
 * A matcher for the custdat mainframe file (version that starts with an RDW).
 * <p/>
 * The 10 first bytes of a record are laid out like this:
 * <ul>
 * <li>First 4 bytes are an integer with a value between 62 and 185</li>
 * <li>Next 6 characters (field customerId) are EBCDIC digits</li>
 * </ul>
 * We consider these 10 characters form a unique signature.
 * 
 */
public class CustdatZosRdwRecordMatcher extends RdwCobolComplexTypeFinder {

    public CustdatZosRdwRecordMatcher() {
        super(new EbcdicCobolContext(), new CobolCustomerData(), "customerId");
    }

}
