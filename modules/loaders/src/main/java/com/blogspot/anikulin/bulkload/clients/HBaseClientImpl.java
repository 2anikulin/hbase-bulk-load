package com.blogspot.anikulin.bulkload.clients;

import com.blogspot.anikulin.bulkload.commons.Utils;
import com.blogspot.anikulin.bulkload.generators.DataGenerator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

import static com.blogspot.anikulin.bulkload.commons.Constants.*;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 *
 * This implementation fills HBase table
 */
public class HBaseClientImpl implements HBaseClient {

    private static final byte[] COLUMN_FAMILY_NAME_BYTES = Bytes.toBytes(COLUMN_FAMILY_NAME);
    private static final byte[] COLUMN_QUALIFIER_DESCRIPTION_BYTES = Bytes.toBytes(COLUMN_QUALIFIER_DESCRIPTION);
    private static final byte[] COLUMN_QUALIFIER_INDEX_BYTES = Bytes.toBytes(COLUMN_QUALIFIER_INDEX);

    private final HTable table;
    private final byte[] rowData;

    public HBaseClientImpl(String zookeeper, String tableName) throws IOException {
        rowData = Bytes.toBytes(DataGenerator.getRowData());

        Configuration hBaseConfiguration = HBaseConfiguration.create();
        hBaseConfiguration.set(ZOOKEEPER_QUORUM, zookeeper);

        table = new HTable(hBaseConfiguration, tableName);
        table.setAutoFlush(false);
    }

    /**
     * Fills HBase table incrementally
     * from start key to end key
     *
     * @param keyStart
     * @param keyEnd
     * @throws IOException
     */
    @Override
    public void send(long keyStart, long keyEnd) throws IOException {

        for (long i = keyStart; i <= keyEnd; i++) {
            String rowKey = Long.toString(i);

            Put put = new Put(Utils.getHash(rowKey));
            put.add(COLUMN_FAMILY_NAME_BYTES, COLUMN_QUALIFIER_INDEX_BYTES, Bytes.toBytes(rowKey));
            put.add(COLUMN_FAMILY_NAME_BYTES, COLUMN_QUALIFIER_DESCRIPTION_BYTES, rowData);

            table.put(put);
        }
        table.flushCommits();
    }

    @Override
    public void close() throws IOException {
        table.close();
    }
}
