package com.blogspot.anikulin.bulkload.mrjob;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import static com.blogspot.anikulin.bulkload.commons.Constants.*;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;

/**
 * @author Anatoliy Nikulin
 * email 2anikulin@gmail.com
 *
 * Mapper unit test
 */
public class BulkLoadJobTest {

    private MapDriver<LongWritable, Text, ImmutableBytesWritable, Put> mapDriver;

    private static final byte[] COLUMN_FAMILY_NAME_BYTES = Bytes.toBytes(COLUMN_FAMILY_NAME);
    private static final byte[] COLUMN_QUALIFIER_DESCRIPTION_BYTES = Bytes.toBytes(COLUMN_QUALIFIER_DESCRIPTION);
    private static final byte[] COLUMN_QUALIFIER_INDEX_BYTES = Bytes.toBytes(COLUMN_QUALIFIER_INDEX);

    private static final String KEY = "key";
    private static final String VALUE = "value";
    private static final String ROW_LINE = KEY + '\t' + VALUE;
    private static final byte[] HASHED_ROW_KEY = {60,110,11,-118,-100,21,34,74,-126,40,-71,-87,-116,-95,83,29};

    @Before
    public void setUp() {
        BulkLoadJob.DataMapper mapper = new BulkLoadJob.DataMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    @Test
    public void mapperTest() throws IOException {

        mapDriver.withInput(new LongWritable(0), new Text(Bytes.toBytes(ROW_LINE)));

        List<Pair<ImmutableBytesWritable,Put>> ret = mapDriver.run();

        assertTrue(ret.size() == 1);

        Put retPut = ret.get(0).getSecond();

        assertTrue(
                Bytes.compareTo(HASHED_ROW_KEY, retPut.getRow()) == 0
        );

        assertTrue(
                retPut.has(COLUMN_FAMILY_NAME_BYTES, COLUMN_QUALIFIER_INDEX_BYTES, Bytes.toBytes(KEY))
        );

        assertTrue(
                retPut.has(COLUMN_FAMILY_NAME_BYTES, COLUMN_QUALIFIER_DESCRIPTION_BYTES, Bytes.toBytes(VALUE))
        );
    }
}
