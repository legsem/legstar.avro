package com.legstar.avro.cob2avro;

import java.nio.ByteBuffer;

import com.legstar.avro.cob2avro.AbstractZosRecordMatcher;

/**
 * A matcher for the custdat mainframe file (version that does not start with an RDW).
 *
 */
public class CustdatZosRecordMatcher extends AbstractZosRecordMatcher {

    /**
     * The 58 first bytes of a record are laid out like this:
     * <ul>
     * <li>First 6 characters are EBCDIC digits</li>
     * <li>Next 48 characters are EBCDIC text</li>
     * <li>Last 4 bytes are an integer with a value between 0 and 5</li>
     * </ul>
     * 
     * @param hostData the bytes to test for a match
     * @param start where to start matching
     * @param length total length of the buffer
     * @return true if the start position matches the start of a CUSTOMER-DATA
     *         record.
     */
    public boolean doMatch(byte[] hostData, int start, int length) {
        int pos = start;
        for (int i = pos; i < pos + 6; i++) {
            int v = hostData[i] & 0xFF;
            if (v < 240 || v > 249) {
                return false;
            }
        }
        pos += 6;
        
        for (int i = pos; i < pos + 48; i++) {
            int v = hostData[i] & 0xFF;
            if (v < 64 || v > 254) {
                return false;
            }
        }
        pos += 48;

        int transactionNbr = ByteBuffer.wrap(hostData, pos, 4).getInt();
        if (transactionNbr < 0 || transactionNbr > 5) {
            return false;
        }
        return true;
    }

    public int signatureLen() {
        return 58;
    }

}
