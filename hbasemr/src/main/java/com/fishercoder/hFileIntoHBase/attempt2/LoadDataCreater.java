package com.fishercoder.hFileIntoHBase.attempt2;

/**
 * Created by stevesun on 6/1/17.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;

public class LoadDataCreater {
    /**This is also giving me below error:
     *
     * Exception in thread "main" java.io.IOException: Mkdirs failed to create /user/stevesun/hbase-staging (exists=false, cwd=file:/Users/stevesun/personal_dev/HBaseMapReduceExample/hbasemr)
     at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:440)
     at org.apache.hadoop.fs.ChecksumFileSystem.create(ChecksumFileSystem.java:426)
     at org.apache.hadoop.fs.FileSystem.create(FileSystem.java:906)
     at org.apache.hadoop.io.SequenceFile$Writer.<init>(SequenceFile.java:1071)
     at org.apache.hadoop.io.SequenceFile$RecordCompressWriter.<init>(SequenceFile.java:1371)
     at org.apache.hadoop.io.SequenceFile.createWriter(SequenceFile.java:272)
     at org.apache.hadoop.io.SequenceFile.createWriter(SequenceFile.java:294)
     at org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.writePartitions(HFileOutputFormat2.java:335)
     at org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.configurePartitioner(HFileOutputFormat2.java:596)
     at org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.configureIncrementalLoad(HFileOutputFormat2.java:440)
     at org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.configureIncrementalLoad(HFileOutputFormat2.java:405)
     at org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2.configureIncrementalLoad(HFileOutputFormat2.java:367)
     at com.fishercoder.hFileIntoHBase.attempt2.LoadDataCreater.main(LoadDataCreater.java:34)*/

    private static final String HBASE_TABLE_NAME = "steve11";

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();

        Job job = new Job(conf, "bulkload sample");
        job.setJarByClass(LoadDataCreater.class);

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("fam"), Bytes.toBytes("col"));

        TableMapReduceUtil.initTableMapperJob(HBASE_TABLE_NAME, scan, LoadDataCreateMapper.class, ImmutableBytesWritable.class, Put.class, job);
        HFileOutputFormat2.setOutputPath(job, new Path("/tmp/loadfiles"));
        HFileOutputFormat2.configureIncrementalLoad(job, new HTable(conf, HBASE_TABLE_NAME));

        int status = job.waitForCompletion(true) ? 0 : 1;
        System.exit(status);
    }
}
