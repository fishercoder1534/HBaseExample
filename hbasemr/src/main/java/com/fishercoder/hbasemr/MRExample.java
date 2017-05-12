package com.fishercoder.hbasemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Calendar;


public class MRExample {
    public MRExample() {
    }

    static class MyMapper extends TableMapper<LongWritable, LongWritable> {
        private LongWritable ONE = new LongWritable(1);

        @Override
        protected void map(ImmutableBytesWritable rowkey, Result columns, Context context)
                throws IOException, InterruptedException {

            // Get the timestamp from the row key
            long timestamp = 0; //ExampleSetup.getTimestampFromRowKey(rowkey.get());

            // Get hour of day
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(timestamp);
            int hourOfDay = calendar.get(Calendar.HOUR_OF_DAY);

            // Output the current hour of day and a count of 1
            context.write(new LongWritable(hourOfDay), ONE);
            System.out.println("wrote one...");
        }
    }

    static class MyReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // Add up all of the page views for this hour
            long sum = 0;
            for (LongWritable count : values) {
                sum += count.get();
            }

            // Write out the current hour and the sum
            context.write(key, new LongWritable(sum));
            System.out.println("wrote sum: " + sum);
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println("In here.");
            // Setup Hadoop
            Configuration conf = HBaseConfiguration.create();
            Job job = Job.getInstance(conf, "access_logs");
            job.setJarByClass(MRExample.class);

            // Create a scan
            Scan scan = new Scan();

            // Configure the Map process to use HBase
            TableMapReduceUtil.initTableMapperJob(
                "access_logs",              // The name of the table
                scan,                           // The scan to execute against the table
                MyMapper.class,                 // The Mapper class
                LongWritable.class,             // The Mapper output key class
                LongWritable.class,             // The Mapper output value class
                job);                           // The Hadoop job

            // Configure the reducer process
            job.setReducerClass(MyReducer.class);
            job.setCombinerClass(MyReducer.class);

            // Setup the output - we'll write to the file system: HOUR_OF_DAY   PAGE_VIEW_COUNT
            job.setOutputKeyClass(LongWritable.class);
            job.setOutputValueClass(LongWritable.class);
            job.setOutputFormatClass(TextOutputFormat.class);

            // We'll run just one reduce task, but we could run multiple
            job.setNumReduceTasks(1);

            // Write the results to a file in the output directory
            FileOutputFormat.setOutputPath(job, new Path("output"));

            // Execute the job
            System.exit(job.waitForCompletion(true) ? 0 : 1);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
