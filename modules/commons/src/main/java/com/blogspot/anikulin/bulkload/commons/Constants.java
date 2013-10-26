package com.blogspot.anikulin.bulkload.commons;

/**
 * Hardcoded environment constants.
 *
 * @author Anatoliy Nikulin
 * email 2anikulin@gmail.com
 */
public class Constants {

    /**
     * Zookeeper setting.
     */
    public static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

    /**
     * Zookeeper host address.
     */
    public static final String ZOOKEEPER_HOST = "categorizer-hadoop-1";

    /**
     * HBase table name.
     */
    public static final String TABLE_NAME = "bulk_table";

    /**
     * Column family name.
     */
    public static final String COLUMN_FAMILY_NAME = "cf_data";

    /**
     * Column qualifier name.
     */
    public static final String COLUMN_QUALIFIER_DESCRIPTION = "column_description";

    /**
     * Column qualifier name.
     */
    public static final String COLUMN_QUALIFIER_INDEX = "column_index";

    /**
     * HDFS directory.
     */
    public static final String JOB_INPUT_PATH = "/bulkload/input";

    /**
     * HDFS directory.
     */
    public static final String JOB_OUTPUT_PATH = "/bulkload/output";

    /**
     * Divisor.
     */
    public static final long NANO_SECONDS = 1000000000;

}
