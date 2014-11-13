package com.legstar.avro.cob2avro;

public abstract class AbstractZosRecordMatcher implements ZosRecordMatcher {

    public int match(byte[] signatureBytes) {
        return match(signatureBytes, 0, signatureBytes.length);
    }

    public int match(byte[] signatureBytes, int start, int length) {
        int pos = start;
        while (length - pos >= signatureLen()) {
            if (doMatch(signatureBytes, pos, length)) {
                return pos;
            }
            pos++;
        }
        return -1;
    }

    public abstract boolean doMatch(byte[] hostData, int start, int length);

}
