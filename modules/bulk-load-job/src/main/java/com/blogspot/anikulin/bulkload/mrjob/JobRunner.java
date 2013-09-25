package com.blogspot.anikulin.bulkload.mrjob;

import com.blogspot.anikulin.bulkload.commons.Constants;
import com.blogspot.anikulin.bulkload.commons.Utils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static com.blogspot.anikulin.bulkload.commons.Constants.*;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 *
 * Hadoop Job Runner implementation
 * It prepare data for map reduce.
 * Creates HBase table if doesn't exists
 * Sequentially runs job and bulk load function
 */
public class JobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(JobRunner.class);
    private static final String JOB_NAME = "Data loading Job";
    private static final short FULL_GRANTS = (short)0777;
    private static final int   WAIT_TIME = 1000;


    public static void main(String[] args) {
        try {
            LOG.info("Runner started");

            Configuration hBaseConfiguration = HBaseConfiguration.create();
            hBaseConfiguration.set(ZOOKEEPER_QUORUM, ZOOKEEPER_HOST);

            LOG.info("Zookeeper path: " + ZOOKEEPER_HOST);

            long startTime = System.nanoTime();

            loading(hBaseConfiguration);

            long endTime = System.nanoTime() - startTime;

            LOG.info("Runner finished. Time: {} sec", endTime / Constants.NANO_SECONDS);
        } catch(Exception e) {
            LOG.error("JobRunner fail", e);
        }

        System.exit(0);
    }

    private static void loading(Configuration jobConfiguration) {
        LOG.info("Loading process started");
        LOG.info("Job input path {}", JOB_INPUT_PATH);
        LOG.info("Job output path {}", JOB_OUTPUT_PATH);

        HTable dataTable = null;

        try {
            if (!Utils.isHTableExists(TABLE_NAME, jobConfiguration)) {
                LOG.info("Table {} doesn't exists. Try to create", TABLE_NAME);
                Utils.createHTable(TABLE_NAME, jobConfiguration);
            }

            dataTable = new HTable(jobConfiguration, TABLE_NAME);

            ControlledJob controlledJob = new ControlledJob(
                    BulkLoadJob.createJob(
                            jobConfiguration,
                            dataTable,
                            JOB_INPUT_PATH,
                            JOB_OUTPUT_PATH
                    ),
                    null
            );

            JobControl jobController = new JobControl(JOB_NAME);
            jobController.addJob(controlledJob);

            LOG.info("Data collecting started");

            Thread thread = new Thread(jobController);
            thread.start();

            Object monitor = new Object();
            while (!jobController.allFinished()) {
                synchronized (monitor) {
                    try {
                        monitor.wait(WAIT_TIME);
                    } catch(InterruptedException e) {
                        LOG.error("JobsRunner was interrupted", e);
                    }
                }
            }

            LOG.info("Data collecting has finished");

            List<ControlledJob> successJobs = jobController.getSuccessfulJobList();
            if (successJobs.contains(controlledJob)) {
                setFullPermissions(JOB_OUTPUT_PATH);

                LOG.info("Bulk-load process started");

                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(jobConfiguration);
                loader.doBulkLoad(
                        new Path(JOB_OUTPUT_PATH),
                        dataTable
                );

                LOG.info("Bulk-load process has finished");
            }

            if (jobController.getFailedJobList().size() > 0 ) {
                LOG.error("Some jobs has not completed!");
            }

        } catch(Exception e) {
            LOG.error("JobRunner - fail", e);
        } finally {
            Utils.close(dataTable);
        }
    }

    /**
     * Sets full 777 permissions on each file
     *
     * @param paths  Array of HDFS-paths
     * @throws IOException
     */
    private static void setFullPermissions(String... paths) throws IOException {
        LOG.info("Start change permissions");

        FileSystem system = Utils.getHDFSFileSystem();

        if (system != null) {
            try {
                for (String path : paths) {
                    Path uriPath = new Path(path + Path.SEPARATOR + COLUMN_FAMILY_NAME);
                    if (!system.exists(uriPath)) {
                        LOG.info("Path doesn't exists: " + uriPath.toString());
                        continue;
                    }

                    LOG.info("Try to set new permissions for folder: " + uriPath.toString());
                    system.setPermission(uriPath, FsPermission.createImmutable(FULL_GRANTS));

                    RemoteIterator<LocatedFileStatus> fileStatuses = system.listLocatedStatus(uriPath);

                    for (LocatedFileStatus status; fileStatuses.hasNext();) {
                        status = fileStatuses.next();
                        if (status != null) {
                            LOG.info("Try to set new permissions for file: " + status.getPath());
                            system.setPermission(status.getPath(), FsPermission.createImmutable(FULL_GRANTS));
                        }
                    }
                }
            } finally {
                Utils.close(system);
            }
        }
    }
}
