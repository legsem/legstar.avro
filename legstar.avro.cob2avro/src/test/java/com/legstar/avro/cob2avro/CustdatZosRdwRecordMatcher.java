package com.legstar.avro.cob2avro;

import com.legstar.avro.cob2avro.AbstractZosRecordMatcher;
import com.legstar.avro.cob2avro.ZosVarRdwDatumReader;

/**
 * A matcher for the custdat mainframe file (version that starts with an RDW).
 *
 */
public class CustdatZosRdwRecordMatcher extends AbstractZosRecordMatcher {

    /**
     * The 10 first bytes of a record are laid out like this:
     * <ul>
     * <li>First 4 bytes are an integer with a value between 62 and 185</li>
     * <li>Next 6 characters are EBCDIC digits</li>
     * </ul>
     * 
     * @param hostData the bytes to test for a match
     * @param start where to start matching
     * @param length total length of the buffer
     * @return true if the start position matches the start of a CUSTOMER-DATA
     *         record.
     */
    public boolean doMatch(byte[] hostData, int start, int length) {
        int rdw = ZosVarRdwDatumReader.getRawRdw(hostData, start, length);
        if (rdw < 58 || rdw > 181) {
            return false;
        }
        for (int i = start + 4; i < start + 10; i++) {
            int v = hostData[i] & 0xFF;
            if (v < 240 || v > 249) {
                return false;
            }
        }
        return true;
    }

    public int signatureLen() {
        return 10;
    }

}
