package com.fishercoder.hFileIntoHBase.attempt4;

import com.fishercoder.hFileIntoHBase.attempt2.LoadDataCreateMapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by stevesun on 6/4/17.
 */
public class ConvertToHFiles extends Configured implements Tool {

    private static final Log LOG = LogFactory.getLog(ConvertToHFiles.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ConvertToHFiles(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {
            System.out.println("In try...");
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "10.211.126.134,10.211.126.103,10.211.126.130");
            Connection connection = ConnectionFactory.createConnection(conf);
            System.out.println("got connection");

            String inputPath = "/tmp/nuggets_from_Hive/part-00000";
            String outputPath = "/tmp/mytemp";
            final TableName tableName = TableName.valueOf("steve1");
            System.out.println("got table steve1");

            // tag::SETUP[]
            Table table = connection.getTable(tableName);

            Job job = Job.getInstance(conf, "ConvertToHFiles: Convert CSV to HFiles");
            System.out.println("job is setup...");

            HFileOutputFormat2.configureIncrementalLoad(job, table,
                    connection.getRegionLocator(tableName)); // <1>
            System.out.println("done configuring incremental load...");
            job.setInputFormatClass(TextInputFormat.class); // <2>

            job.setJarByClass(ConvertToHFiles.class); // <3>
//            job.setJar("/home/cloudera/ahae/target/ahae.jar"); // <3>

            job.setMapperClass(LoadDataCreateMapper.class); // <4>
            job.setMapOutputKeyClass(ImmutableBytesWritable.class); // <5>
            job.setMapOutputValueClass(KeyValue.class); // <6>

            FileInputFormat.setInputPaths(job, inputPath);
            HFileOutputFormat2.setOutputPath(job, new Path(outputPath));
            System.out.println("Setup complete...");
            // end::SETUP[]

            if (!job.waitForCompletion(true)) {
                LOG.error("Failure");
            } else {
                LOG.info("Success");
                return 0;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 1;
    }
}

