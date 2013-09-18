package com.blogspot.anikulin.bulkload.loaders;

import com.blogspot.anikulin.bulkload.clients.HBaseClient;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 */
public class HBaseLoaderTest {

    @Test
    public void mainTest() throws IOException {
        HBaseLoader loader = mock(HBaseLoader.class);
        final HBaseClientMock clientMock = new HBaseClientMock();

        when(
                loader.createClient(Mockito.anyString(), Mockito.anyString())
        ).thenReturn(
                clientMock
        );

        loader.main(new String[]{"0", "1000000"});
    }
}

class HBaseClientMock implements HBaseClient {

    private static final List<Pair> keys = new CopyOnWriteArrayList<Pair>();
    private static final AtomicInteger closeCounter = new AtomicInteger();

    @Override
    public void send(long keyStart, long keyEnd) throws IOException {
        keys.add(new Pair(keyStart, keyEnd));
    }

    @Override
    public void close() throws IOException {
        closeCounter.getAndIncrement();
    }

    public List<Pair> getKeys() {
        return keys;
    }

    public int getCloseCount() {
        return closeCounter.get();
    }

    public static class Pair {
        private long left;
        private long right;

        public Pair(long left, long right) {
            this.left = left;
            this.right = right;
        }

        public long getLeft() {
            return left;
        }

        public long getRight() {
            return right;
        }
    }
}

