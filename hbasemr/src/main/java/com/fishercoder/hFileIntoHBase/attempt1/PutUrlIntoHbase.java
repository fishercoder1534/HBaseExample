package com.fishercoder.hFileIntoHBase.attempt1;

/**
 * Created by stevesun on 6/1/17.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.PutSortReducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class PutUrlIntoHbase extends Configured implements Tool {
    /**
     * Run it on Cluster, it might be working fine.
     *
     * This program isn't working yet, it's giving me this error:
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
     at com.fishercoder.hFileIntoHBase.attempt1.PutUrlIntoHbase.run(PutUrlIntoHbase.java:41)
     at org.apache.hadoop.util.ToolRunner.run(ToolRunner.java:70)
     at com.fishercoder.hFileIntoHBase.attempt1.PutUrlIntoHbase.main(PutUrlIntoHbase.java:51)

     I asked on Stackoverflow: https://stackoverflow.com/questions/44318693/java-io-ioexception-mkdirs-failed-to-create-when-running-mapreduce-job
     no answers yet.*/

    private static final String MAPRED_JOB_NAME = "mapred.job.name";
    private static final String HBASE_TABLE = "mapred.job.name";

    public int run(String[] arg0) throws Exception {
        System.out.println("In run.........");
        Configuration conf = new Configuration();
        conf.set(MAPRED_JOB_NAME, "steve_test");
        conf.set(HBASE_TABLE, "steve1");
//        conf.set("hbase.master", "10.211.126.103:60000");
        conf.set("hbase.master", "localhost:60000");
//        conf.set("hbase.zookeeper.quorum", "10.211.126.103");
        Job job = Job.getInstance(conf, conf.get(MAPRED_JOB_NAME));
        String output_table = conf.get(HBASE_TABLE);
        System.out.println("output_table = " + output_table);

        job.setJarByClass(PutUrlIntoHbase.class);
        job.setMapperClass(PutUrlIntoHbaseMapper.class);
        job.setReducerClass(PutSortReducer.class);

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(Put.class);

        HTable table = new HTable(conf, output_table);
        job.setOutputFormatClass(HFileOutputFormat2.class);
        System.out.println("job is set...");
        HFileOutputFormat2.configureIncrementalLoad(job, table);
        System.out.println("load configured...");

        if (job.waitForCompletion(true) && job.isSuccessful()) {
            return 0;
        }
        return -1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        int res = ToolRunner.run(conf, new PutUrlIntoHbase(), args);
        System.exit(res);
    }
}
