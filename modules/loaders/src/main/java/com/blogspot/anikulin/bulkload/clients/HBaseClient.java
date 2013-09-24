package com.blogspot.anikulin.bulkload.clients;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 *
 * Facade for HBase client
 */
public interface HBaseClient extends Closeable {

    /**
     * Fills HBase table incrementally
     * from start key to end
     *
     * @param keyStart
     * @param keyEnd
     * @throws IOException
     */
    void send(long keyStart, long keyEnd) throws IOException;

}
