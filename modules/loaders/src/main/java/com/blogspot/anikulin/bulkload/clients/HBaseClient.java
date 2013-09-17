package com.blogspot.anikulin.bulkload.clients;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 */
public interface HBaseClient extends Closeable {

    void send(long keyStart, long keyEnd) throws IOException;

}
