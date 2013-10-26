package com.blogspot.anikulin.bulkload.loaders;

import com.blogspot.anikulin.bulkload.clients.HBaseClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;
import static org.powermock.api.support.membermodification.MemberMatcher.method;
import static org.powermock.api.support.membermodification.MemberModifier.stub;

/**
 * @author Anatoliy Nikulin
 * email 2anikulin@gmail.com
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(HBaseLoader.class)
@PowerMockIgnore({"javax.management.*"})
public class HBaseLoaderTest {

    private static String STUB_METHOD = "createClient";
    private static String KEY_START = "0";
    private static String KEY_END = "1000000";

    @Test
    public void mainTest() throws Exception {

        final HBaseClientMock clientMock = new HBaseClientMock();

        stub(method(HBaseLoader.class, STUB_METHOD)).toReturn(clientMock);

        HBaseLoader.main(new String[]{KEY_START, KEY_END});

        assertTrue(clientMock.getCloseCount() == 8);

        assertTrue(clientMock.getKeys().get(0L) == 124999);
        assertTrue(clientMock.getKeys().get(125000L) == 249999);
        assertTrue(clientMock.getKeys().get(250000L) == 374999);
        assertTrue(clientMock.getKeys().get(375000L) == 499999);
        assertTrue(clientMock.getKeys().get(500000L) == 624999);
        assertTrue(clientMock.getKeys().get(625000L) == 749999);
        assertTrue(clientMock.getKeys().get(750000L) == 874999);
        assertTrue(clientMock.getKeys().get(875000L) == 999999);
    }
}

/**
 * HBaseClient Mock implementation
 */
class HBaseClientMock implements HBaseClient {

    private static final Map<Long, Long> keys = new ConcurrentHashMap<Long, Long>();
    private static final AtomicInteger closeCounter = new AtomicInteger();

    @Override
    public void send(long keyStart, long keyEnd) throws IOException {
        keys.put(keyStart, keyEnd);
    }

    @Override
    public void close() throws IOException {
        closeCounter.getAndIncrement();
    }

    public Map<Long, Long> getKeys() {
        return keys;
    }

    public int getCloseCount() {
        return closeCounter.get();
    }
}

