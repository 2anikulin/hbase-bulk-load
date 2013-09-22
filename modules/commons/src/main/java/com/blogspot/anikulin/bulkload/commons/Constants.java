package com.blogspot.anikulin.bulkload.commons;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 */
public class Constants {

    public static final String ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    public static final String ZOOKEEPER_HOST = "categorizer-hadoop-1";

    public static final String TABLE_NAME = "bulk_table";
    public static final String COLUMN_FAMILY_NAME = "cf_data";
    public static final String COLUMN_QUALIFIER_DESCRIPTION = "column_description";
    public static final String COLUMN_QUALIFIER_INDEX = "column_index";

    public static final String JOB_INPUT_PATH = "/bulkload/input";
    public static final String JOB_OUTPUT_PATH = "/bulkload/output";

    public static final long NANO_SECONDS = 1000000000;

}
