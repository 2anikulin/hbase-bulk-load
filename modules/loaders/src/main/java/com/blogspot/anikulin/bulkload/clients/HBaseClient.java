package com.blogspot.anikulin.bulkload.clients;

import java.io.Closeable;
import java.io.IOException;

/**
 * Facade for HBase client.
 *
 * @author Anatoliy Nikulin
 * email 2anikulin@gmail.com
 */
public interface HBaseClient extends Closeable {

    /**
     * Fills HBase table incrementally.
     * from start key to end.
     *
     * @param keyStart start.
     * @param keyEnd finish.
     * @throws IOException .
     */
    void send(final long keyStart, final long keyEnd) throws IOException;

}
