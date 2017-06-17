package com.fishercoder.hFileIntoHBase.mapreduceSolution;

/**
 * Created by stevesun on 6/16/17.
 */

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Random;

/**
 * Created by stevesun on 6/8/17.
 */
public class HFileImporter implements Serializable {

    public static final String COL_FAMILY = "pd";
    public static final String COLUMN = "bf";
    private static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String APP_NAME = "FisherCoder-HFile-Importer";

    private static Map<String, String> JSON_HISTORY_S3_PATHS =
            ImmutableMap.<String, String>builder()
//                    .put("prod", "s3a://BUCKET_NAME")
                    .put("prod", "s3a://s3-cdp-prod-hive/indexing_platform/solr_json_history/")
                    .put("dev", "s3a://BUCKET_NAME")
                    .put("sample", "s3a://BUCKET_NAME")
                    .put("sample-prod", "s3a://s3-cdp-prod-hive/temp/version")
                    .build();

    public static class HFileImporterParams implements Serializable {
        @Parameter(names = { "--textFilePath" }, required = false,
                description = "text files generated output path.")
        public String textFilePath = "/nuggets/" + new Random().nextInt(1000000);

        @Parameter(names = { "--hfilesOutputPath" }, required = false,
                description = "HFiles generated output path.")
        public String hfilesOutputPath = "/nuggets/" + new Random().nextInt(1000000);

        @Parameter(names = { "--hbaseMasterGateway" }, required = false,
                description = "Hbase master node IP.")
        public String hbaseMasterGateway = "XXXX:60000";

        @Parameter(names = { "--zkquorum" }, required = false,
                description = "zoo keeper's quorum")
        public String zkQuorum = "XXXX:2181";

        @Parameter(names = { "--hbaseTargetTable" }, required = false,
                description = "Hbase target table to be written into and to get "
                        + "all metadata, e.g. region servers from to generate HFiles.")
        public String hbaseTargetTable = "hbase-table";

        @Parameter(names = { "--latestdatehour" }, required = false,
                description = "Latest date and hour to read Hive table from in S3.")
        public String latestDateHour = "";

        @Parameter(names = { "--shards" }, required = false, description = "Total number of shards")
        public int shards = 2;

        @Parameter(names = { "--local" }, required = false, arity = 1,
                description = "Whether to run spark in local mode.")
        public boolean local = false;

        @Parameter(names = { "--hfile" }, required = false, arity = 1,
                description = "Whether to use HFile to load into HBase or not, "
                        + "if false, we'll create connections"
                        + "to HBase and do PUTs into HBase")
        public boolean hfile = false;

        @Parameter(names = { "--source" }, required = false, description = "The source of s3")
        public String source = "sample";

        @Parameter(names = { "--dataSourceFormat" }, required = false, description = "The data source format of s3")
        public String dataSourceFormat = "orc";//or "csv", etc.
    }

    private HFileImporterParams hFileImporterParams;

    public HFileImporter(HFileImporterParams params) {
        hFileImporterParams = params;
    }

    public static void main(String[]  args) throws Exception {
        HFileImporterParams params = new HFileImporterParams();
        new JCommander(params, args);
        HFileImporter hFileImporter = new HFileImporter(params);
        hFileImporter.importIntoHBase();
    }

    protected void importIntoHBase() throws Exception {
        JavaRDD<Row> rdd = readJsonTable();
        if (hFileImporterParams.hfile) {
            saveAsTextFile(rdd);
            generateHFiles(hFileImporterParams.local, hFileImporterParams.zkQuorum,
                    hFileImporterParams.textFilePath, hFileImporterParams.hfilesOutputPath,
                    hFileImporterParams.hbaseTargetTable);
        }
//		sanityCheck();
    }

    protected void saveAsTextFile(JavaRDD<Row> rdd) throws IOException {
        try {
            JavaRDD<String> javaRdd = rdd.map(
                    new Function<Row, String>() {
                        @Override
                        public String call(Row row) throws Exception {
                            return row.get(0) + "\t" + row.get(1);
                            //use "\t" as delimiter since ",#;" are all being used in the text
                        }
                    });
            javaRdd.saveAsTextFile(hFileImporterParams.textFilePath);

        } catch (Exception e) {
            System.out.println("Caught exception: " + e);
        }
        System.out.println("Finished saveAsTextFile to: " + hFileImporterParams.textFilePath);
    }

    public JavaRDD<Row> readJsonTable() {
        SparkSession.Builder builder = SparkSession.builder().appName(APP_NAME);
        if (hFileImporterParams.local) {
            builder.master("local");
        }

        SparkSession spark = builder.getOrCreate();

        SparkContext sparkContext = spark.sparkContext();

        // this is important. need to set the endpoint to ap-northeast-2
        sparkContext.hadoopConfiguration()
                .set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com");

        Dataset<Row> rows = null;
        rows = spark.read().format(hFileImporterParams.dataSourceFormat)
                .load(JSON_HISTORY_S3_PATHS.get(hFileImporterParams.source) + hFileImporterParams.latestDateHour);

        return rows.toJavaRDD();
    }

    public static class LoadDataMapper extends
            Mapper<LongWritable, Text, ImmutableBytesWritable, Cell> {

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] line = value.toString().split("\t");

            ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
            byte[] rowKeyBytes = Bytes.toBytes(line[0]);
            rowKey.set(rowKeyBytes);

            KeyValue kv = new KeyValue(rowKeyBytes,
                    Bytes.toBytes(COL_FAMILY),
                    Bytes.toBytes(COLUMN),
                    new String(line[0] + line[1]).getBytes());

            context.write(rowKey, kv);
        }

    }

    protected static void generateHFiles(boolean local, String zkQuorum,
                                         String textfilePath, String hfileOutputPath, String hbaseTargetTable) throws Exception {
        try {
            Configuration hbaseConfig = HBaseConfiguration.create();
            if (!local) {
                hbaseConfig.set(HBASE_ZOOKEEPER_QUORUM, zkQuorum);
            }

            Job job = Job.getInstance(hbaseConfig, "generateHFiles");
            job.setJarByClass(HFileImporter.class);
            job.setMapperClass(LoadDataMapper.class);
            job.setReducerClass(KeyValueSortReducer.class);

            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);

            job.setOutputFormatClass(TextOutputFormat.class);
            job.setPartitionerClass(SimpleTotalOrderPartitioner.class);

            FileInputFormat.setInputPaths(job, textfilePath);

            HFileOutputFormat2.setOutputPath(job, new org.apache.hadoop.fs.Path(hfileOutputPath));
            System.out.println("hfileOutputPath = " + hfileOutputPath);

            Connection connection = ConnectionFactory.createConnection(hbaseConfig);
            final TableName tableName = TableName.valueOf(hbaseTargetTable);
            Table table = connection.getTable(tableName);

            long startTime = System.currentTimeMillis();
            HFileOutputFormat2.configureIncrementalLoad(job, table,
                    connection.getRegionLocator(tableName));
            System.out.println("done configuring incremental load...");

            if (!job.waitForCompletion(true)) {
                System.out.println("Failure");
            } else {
                System.out.println("Success, it took "
                        + (System.currentTimeMillis() - startTime) / 1000
                        + " seconds to generate HFiles and HFiles are saved at: " + hfileOutputPath);
            }

            /**below takes too long, use command line: e.g.
             * $hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles
             * /tmp/44 'nuggets-sample3'
             * instead for now*/
            //       HTable hTable = new HTable(hbaseConfig, importerParams.hbaseTargetTable);
            //       LoadIncrementalHFiles loader = new LoadIncrementalHFiles(hbaseConfig);
            //       loader.doBulkLoad(new org.apache.hadoop.fs.Path(HFILE_OUTPUT_PATH), hTable);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
