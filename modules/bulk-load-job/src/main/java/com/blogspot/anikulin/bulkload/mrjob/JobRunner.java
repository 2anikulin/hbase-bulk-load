package com.blogspot.anikulin.bulkload.mrjob;

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

import static com.blogspot.anikulin.bulkload.mrjob.Constants.*;

/**
 * @author Anatoliy Nikulin
 * @email 2anikulin@gmail.com
 */
public class JobRunner {

    private static final Logger LOG = LoggerFactory.getLogger(JobRunner.class);
    private static final String JOB_NAME = "Data loading Job";

    public static void main(String[] args) {
        try {
            LOG.info("Runner started");

            //Configuration jobConfiguration = new Configuration();
            Configuration hBaseConfiguration = HBaseConfiguration.create(/*jobConfiguration*/);
            hBaseConfiguration.set(ZOOKEEPER_QUORUM, ZOOKEEPER_HOST);

            LOG.info("Zookeeper path: " + ZOOKEEPER_HOST);

            loading(/*jobConfiguration,*/ hBaseConfiguration);

            LOG.info("Runner finished");
        } catch(Exception e) {
            LOG.error("JobRunner fail", e);
        }

        System.exit(0);
    }

    private static void loading(/*Configuration jobConfiguration, */Configuration jobConfiguration) {
        LOG.info("Start loading process");

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

            LOG.info("Start data collection");

            Thread thread = new Thread(jobController);
            thread.start();
            thread.join();

            /*
            while (!jobController.allFinished()) {
                synchronized (lock) {
                    try {
                        lock.wait(WAIT_PERIOD);
                    } catch(InterruptedException e) {
                        LOG.error("[JobsRunner - fail]", e);
                    }
                }
            } */

            List<ControlledJob> successJobs = jobController.getSuccessfulJobList();
            if (successJobs.contains(controlledJob)) {
                setFullPermissions(JOB_OUTPUT_PATH);

                LOG.info("Start bulk-load process");
                LoadIncrementalHFiles loader = new LoadIncrementalHFiles(jobConfiguration);
                loader.doBulkLoad(
                        new Path(JOB_OUTPUT_PATH),
                        dataTable
                );

                LOG.info("Bulk load finished");
            }

            if (jobController.getFailedJobList().size() > 0 ) {
                LOG.error("Some jobs has not completed");
            }

        } catch(Throwable e) {
            LOG.error("[JobRunner - fail]", e);
        } finally {
            closeTable(dataTable);
        }
    }

    private static void closeTable(HTable table) {
        if (table != null) {
            try {
                table.close();
            } catch(IOException e) {
                LOG.error("Can't close HBase table", e);
            }
        }
    }

    private static void setFullPermissions(String... paths) throws IOException {
        LOG.info("Change permissions");
        FileSystem system = Utils.getHDFSFileSystem();

        if (system != null) {
            for (String path : paths) {
                Path uriPath = new Path(path + Path.SEPARATOR + MR_SYSTEM_OUTPUT_FOLDER);
                if (!system.exists(uriPath)) {
                    LOG.info("Path not exists: " + uriPath.toString());
                    continue;
                }

                LOG.info("Try set new permissions on folder " + uriPath.toString());
                system.setPermission(uriPath, FsPermission.createImmutable((short)0777));

                RemoteIterator<LocatedFileStatus> fileStatuses = system.listLocatedStatus(uriPath);

                for (LocatedFileStatus status; fileStatuses.hasNext();) {
                    status = fileStatuses.next();
                    if (status != null) {
                        LOG.info("Try set new permissions on " + status.getPath());
                        system.setPermission(status.getPath(), FsPermission.createImmutable((short)0777));
                    }
                }
            }

            system.close();
        }
    }
}
