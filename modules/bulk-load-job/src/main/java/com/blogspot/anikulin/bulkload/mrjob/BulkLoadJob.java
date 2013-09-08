package com.blogspot.anikulin.bulkload.mrjob;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import static com.blogspot.anikulin.bulkload.mrjob.Constants.*;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 */

public class BulkLoadJob extends Configured implements Tool {

    private static final Logger LOG = LoggerFactory.getLogger(BulkLoadJob.class);
    private static final String JOB_NAME = "HBase bulk-load";

    @Override
    public int run(String[] strings) throws Exception {
        throw new NotImplementedException("Method not implemented");
    }

    public static Job createJob(Configuration configuration, HTable hTable, String inputPath, String outputPath)
            throws IOException {

        LOG.info("Job \"{}\" initializing...", JOB_NAME);

        Job job = new Job(configuration, JOB_NAME);
        job.setJarByClass(BulkLoadJob.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        job.setMapperClass(DataMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(HFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, inputPath);
        HFileOutputFormat.setOutputPath(job, new Path(outputPath));

        //It needs for autoconfiguring partitioner and reducer
        //and it doesn't load any data to Table
        HFileOutputFormat.configureIncrementalLoad(job, hTable);

        LOG.info("Job \"{}\" created", JOB_NAME);

        return job;
    }

    public static enum Counters {
        WRONG_DATA_FORMAT_COUNTER
    }

    public static class DataMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

        private static final Logger LOG = LoggerFactory.getLogger(DataMapper.class);

        private static final byte[] COLUMN_FAMILY_NAME_BYTES = Bytes.toBytes(COLUMN_FAMILY_NAME);
        private static final byte[] COLUMN_QUALIFIER_NAME_BYTES = Bytes.toBytes(COLUMN_QUALIFIER_NAME);

        @Override
        public void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

           String[] values = value.toString().split("\t");
            if (values.length == 2) {

                byte[] rowKey = Bytes.toBytes(values[0]);
                Put put = new Put(rowKey);
                put.add(COLUMN_FAMILY_NAME_BYTES, COLUMN_QUALIFIER_NAME_BYTES, Bytes.toBytes(values[1]));

                context.write(new ImmutableBytesWritable(rowKey), put);
            } else {
                context.getCounter(Counters.WRONG_DATA_FORMAT_COUNTER).increment(1);
                LOG.warn("Wrong line format: {}", value);
            }
        }
    }
}
