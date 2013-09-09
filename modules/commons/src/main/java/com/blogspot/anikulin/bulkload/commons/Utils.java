package com.blogspot.anikulin.bulkload.commons;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 */
public class Utils {

    private static final Logger LOG = LoggerFactory.getLogger(Utils.class);
    private static final String FS_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final int REGIONS_COUNT = 10;

    public static FileSystem getHDFSFileSystem() throws IOException {
        LOG.info("Start configuring HDFS connection");

        Configuration conf = new Configuration();
        /*
        FileSystem.get is a static call even if you have two different objects created.
        And if you close() instance, you couldn't create and use it again
        This should solve the problem,
        */
        conf.setBoolean(FS_DISABLE_CACHE, true);
        FileSystem fileSystem = FileSystem.get(conf);

        LOG.info("Connected to HDFS successfully");

        return fileSystem;
    }

    public static byte[] getHash(String value) {
        return DigestUtils.md5(value);
    }

    public static boolean isHTableExists(String tableName, Configuration config) throws IOException {
        HBaseAdmin admin = null;
        boolean ret = false;

        try {
            LOG.info("Check table on exists. Try connect to HBase");

            admin = new HBaseAdmin(config);
            ret = admin.tableExists(tableName);

            LOG.info("Table {} is {}", tableName, ret ? "exists" : "not exists");
        } catch(IOException e) {
            LOG.error("HBase request failed", e);
            throw e;
        } finally {
            close(admin);
        }

        return ret;
    }

    public static void createHTable(String tableName, Configuration config) throws IOException {
        HTableDescriptor descriptor = new HTableDescriptor(
                Bytes.toBytes(tableName)
        );

        descriptor.addFamily(
                new HColumnDescriptor(Constants.COLUMN_FAMILY_NAME)
        );

        HBaseAdmin admin = null;

        try {
            LOG.info("Try connect to HBase");
            admin = new HBaseAdmin(config);

            byte[] startKey = new byte[16];
            Arrays.fill(startKey, (byte) 0);

            byte[] endKey = new byte[16];
            Arrays.fill(endKey, (byte)255);

            admin.createTable(descriptor, startKey, endKey, REGIONS_COUNT);

        } catch(IOException e) {
            LOG.error("Unable to create table", e);
            throw e;
        } finally {
            close(admin);
        }
    }

    private static void close(HBaseAdmin admin) {
        try {
            if (admin != null) {
                admin.close();
            }
        } catch(IOException e) {
            LOG.error("Admin closing - failed", e);
        }
    }

}
