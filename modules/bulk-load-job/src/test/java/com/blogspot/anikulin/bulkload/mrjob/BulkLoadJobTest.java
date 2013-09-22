package com.blogspot.anikulin.bulkload.mrjob;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.thrift2.generated.THBaseService;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 */
public class BulkLoadJobTest {
    private MapDriver<LongWritable, Text, ImmutableBytesWritable, Put> mapDriver;

    @Before
    public void setUp() {
        BulkLoadJob.DataMapper mapper = new BulkLoadJob.DataMapper();
        mapDriver = MapDriver.newMapDriver(mapper);
    }

    //@Test
    public void impressionMapperTest() throws IOException {

        mapDriver.withInput(new LongWritable(0), new Text(Bytes.toBytes("sss")));
        mapDriver.withOutput(
                new ImmutableBytesWritable(
                        Bytes.toBytes("xxx")
                ),
                new Put()
        );

        List<Pair<ImmutableBytesWritable,Put>> ret = mapDriver.run();

        /*assertTrue(ret.size() == 1);
        assertTrue(
                checkFields(ret.get(0).getSecond(), jsonObject, HBaseImpression.DATA_COLUMN_FAMILY)
        );   */
    }

    /*
    private boolean checkFields(Put put, JSONObject json, byte[] columnFamily) throws JSONException {
        boolean ret = true;
        for (Iterator iterator = json.keys(); iterator.hasNext(); ) {
            String key = (String) iterator.next();

            List<KeyValue> kv = put.get(HBaseImpression.DATA_COLUMN_FAMILY, Bytes.toBytes(key));

            if (kv.size() != 1 && Bytes.toString(kv.get(0).getValue()).compareTo(json.getString(key)) != 0) {
                ret = false;
                break;
            }
        }

        return ret;
    }  */
}
