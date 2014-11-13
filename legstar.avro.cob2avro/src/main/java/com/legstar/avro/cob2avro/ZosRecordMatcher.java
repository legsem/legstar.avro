package com.legstar.avro.cob2avro;

public interface ZosRecordMatcher {

    /**
     * Lookup for the signature of a Mainframe record.
     * 
     * @param signatureBytes the incoming host data where the matching pattern might
     *            appear
     * @return the position of the match or -1 if there is no match
     */
    int match(byte[] signatureBytes);

    /**
     * Lookup for the signature of a Mainframe record.
     * 
     * @param signatureBytes the incoming host data where the matching pattern might
     *            appear
     * @param start where to start looking in the incoming host data
     * @param length where to stop looking in the incoming host data
     * @return the position of the match or -1 if there is no match
     */
    int match(byte[] signatureBytes, int start, int length);

    /**
     * @return the byte length of the signature (total number of bytes needed to
     *         match the pattern)
     */
    int signatureLen();

}
