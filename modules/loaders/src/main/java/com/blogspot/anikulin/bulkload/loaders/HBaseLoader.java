package com.blogspot.anikulin.bulkload.loaders;


import com.blogspot.anikulin.bulkload.clients.HBaseClient;
import com.blogspot.anikulin.bulkload.clients.HBaseClientImpl;
import com.blogspot.anikulin.bulkload.commons.Constants;
import com.blogspot.anikulin.bulkload.commons.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.blogspot.anikulin.bulkload.commons.Constants.*;

/**
 * HBase loader.
 * Creates thread for each client and fills HBase table
 *
 * @author Anatoliy Nikulin
 * email 2anikulin@gmail.com
 */
public class HBaseLoader {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseLoader.class);
    private static final int DEFAULT_THREADS_COUNT = 8;

    private static final int ZOOKEEPER_ARG = 2;
    private static final int TABLE_ARG = 3;
    private static final int THREADS_COUNT_ARG = 4;

    /**
     * Entry point.
     *
     * [start key]       - from key
     * [end key]         - to key
     *
     *  optional:
     *  [zookeeper host] - IP address or domain name of Zookeeper
     *  [table name]     - HBase table name
     *  [threads count]  - Threads count
     *
     * @param args input arguments
     */
    public static void main(final String[] args) {

        if (args.length < 2) {
            System.out.println(
                    "Wrong input parameters. Usage: [start key] [end key] (optional: [zookeeper host] [table name] [threads count])"
            );
            return;
        }

        long startKey = Long.parseLong(args[0]);
        long endKey = Long.parseLong(args[1]);

        String zookeeper = args.length > ZOOKEEPER_ARG ? args[ZOOKEEPER_ARG] : ZOOKEEPER_HOST;
        String tableName = args.length > TABLE_ARG ? args[TABLE_ARG] : TABLE_NAME;

        int threadsCount = args.length > THREADS_COUNT_ARG ? Integer.parseInt(args[THREADS_COUNT_ARG]) : DEFAULT_THREADS_COUNT;

        LOG.info(
                "Process started with: startKey {}, endKey {}, zookeeper {}, table name {}, threads count {}",
                startKey,
                endKey,
                zookeeper,
                tableName,
                threadsCount
        );

        load(startKey, endKey, zookeeper, tableName, threadsCount);
    }


    /**
     * Splits keys by ranges and fills HBase.
     *
     * @param startKey      start key
     * @param endKey        end key
     * @param zookeeper     zookeeper host name
     * @param tableName     HBase table name
     * @param threadsCount  count of threads
     */
    public static void load(final long startKey,
                            final long endKey,
                            final String zookeeper,
                            final String tableName,
                            final int threadsCount) {

        final CyclicBarrier barrier = new CyclicBarrier(threadsCount);
        final CountDownLatch latch = new CountDownLatch(threadsCount);

        final AtomicLong commonTimeCounter = new AtomicLong(0);
        final AtomicLong keysRangeCounter = new AtomicLong(startKey);
        final long step = (endKey - startKey) / threadsCount;

        ExecutorService executor = Executors.newFixedThreadPool(threadsCount);

        for (int i = 0; i < threadsCount; i++) {
            executor.execute(
                    new Runnable() {
                        @Override
                        public void run() {
                            HBaseClient client = null;
                            try {
                                client = createClient(zookeeper, tableName);
                                long fromKey = keysRangeCounter.getAndAdd(step);
                                long toKey = fromKey + step - 1;

                                LOG.info(
                                        "Thread id: {} started and wait. Key range [{}, {}]",
                                         Thread.currentThread().getId(),
                                         fromKey,
                                         toKey
                                );

                                barrier.await();

                                LOG.info(
                                        "Thread id: {} Going on",
                                         Thread.currentThread().getId()
                                );

                                long startTime = System.nanoTime();

                                client.send(fromKey, toKey);

                                commonTimeCounter.getAndAdd(System.nanoTime() - startTime);
                            } catch (InterruptedException ie) {
                                LOG.error("Thread id: {} was interrupted",Thread.currentThread().getId(), ie);
                                Thread.currentThread().interrupt();
                            } catch (BrokenBarrierException | IOException e) {
                                LOG.error("Thread id: {} failed", Thread.currentThread().getId(), e);
                            } finally {
                                Utils.close(client);
                                latch.countDown();
                            }
                        }
                    }
            );
        }

        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.error("Process was interrupted", e);
            Thread.currentThread().interrupt();
        }

        LOG.info(
                "Process finished. Threads count: {} average time (ms): {}",
                threadsCount,
                commonTimeCounter.get() / threadsCount / Constants.NANO_SECONDS
        );

        executor.shutdownNow();
    }

    private static HBaseClient createClient(final String zookeeper, final String tableName) throws IOException {
        return new HBaseClientImpl(zookeeper, tableName);
    }
}
