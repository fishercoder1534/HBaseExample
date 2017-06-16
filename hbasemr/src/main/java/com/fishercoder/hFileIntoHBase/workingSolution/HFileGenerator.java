package com.fishercoder.hFileIntoHBase.workingSolution;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat2;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

/**
 * Created by stevesun on 6/15/17.
 */
public class HFileGenerator implements Serializable {
    static final Logger logger = LogManager.getLogger(HFileGenerator.class);

    public static final String COL_FAMILY = "pd";
    public static final String COLUMN = "bf";

    private static final String TEXTFILE_PATH = "/fishercoder/"
            + new Random().nextInt(1000000);
    public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
    private static final String APP_NAME = "FisherCoder-HFile-Generator";

    private static Map<String, String> JSON_HISTORY_S3_PATHS =
            ImmutableMap.<String, String>builder()
                    .put("prod", "s3a://BUCKET_NAME")
                    .put("dev", "s3a://BUCKET_NAME")
                    .put("sample", "s3a://BUCKET_NAME")
                    .put("sample-prod", "s3a://s3-cdp-prod-hive/temp/version")
                    .build();

    public static class HFileGeneratorParams implements Serializable {
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

    private HFileGeneratorParams HFileGeneratorParams;

    public HFileGenerator(HFileGeneratorParams params) {
        HFileGeneratorParams = params;
    }

    public static void main(String[]  args) throws Exception {
        HFileGeneratorParams params = new HFileGeneratorParams();
        new JCommander(params, args);
        HFileGenerator hFileGenerator = new HFileGenerator(params);
        hFileGenerator.importIntoHBase();
    }

    protected void importIntoHBase() throws Exception {
        JavaRDD<Row> rdd = readJsonTable();
        if (HFileGeneratorParams.hfile) {
            generateHFilesUsingSpark(rdd);
//			saveAsTextFile(rdd);
        } else {
            putToHBase(rdd);
        }
//		sanityCheck();
    }

    protected void generateHFilesUsingSpark(JavaRDD<Row> rdd) throws Exception {
        JavaPairRDD<ImmutableBytesWritable, KeyValue> javaPairRdd = rdd.mapToPair(
                new PairFunction<Row, ImmutableBytesWritable, KeyValue>() {
                    public Tuple2<ImmutableBytesWritable, KeyValue> call(Row row) throws Exception {
                        String key = (String) row.get(0);
                        String value = (String) row.get(1);

                        ImmutableBytesWritable rowKey = new ImmutableBytesWritable();
                        byte[] rowKeyBytes = Bytes.toBytes(key);
                        rowKey.set(rowKeyBytes);

                        KeyValue keyValue = new KeyValue(rowKeyBytes,
                                Bytes.toBytes(COL_FAMILY),
                                Bytes.toBytes(COLUMN),
                                new String(key + value).getBytes());

                        return new Tuple2<ImmutableBytesWritable, KeyValue>(rowKey, keyValue);
                    }
                });

        Configuration baseConf = HBaseConfiguration.create();
        Configuration conf = new Configuration();
        conf.set(HBASE_ZOOKEEPER_QUORUM, HFileGeneratorParams.zkQuorum);
        Job job = new Job(baseConf, APP_NAME);
        HTable table = new HTable(conf, HFileGeneratorParams.hbaseTargetTable);
        Partitioner partitioner = new IntPartitioner(HFileGeneratorParams.shards);
        JavaPairRDD<ImmutableBytesWritable, KeyValue> repartitionedRdd =
                javaPairRdd.repartitionAndSortWithinPartitions(partitioner);
        HFileOutputFormat2.configureIncrementalLoad(job, table);
        System.out.println("Done configuring incremental load....");

        Configuration config = job.getConfiguration();

        repartitionedRdd.saveAsNewAPIHadoopFile(
                HFileGeneratorParams.hfilesOutputPath,
                ImmutableBytesWritable.class,
                KeyValue.class,
                HFileOutputFormat2.class,
                config
        );
        System.out.println("Saved to HFiles to: " + HFileGeneratorParams.hfilesOutputPath);

    }


    public class IntPartitioner extends Partitioner {
        private final int numPartitions;
        public Map<List<String>, Integer> rangeMap = new HashMap<>();

        public IntPartitioner(int numPartitions) throws IOException {
            this.numPartitions = numPartitions;
            this.rangeMap = getRangeMap();
        }

        private Map<List<String>, Integer> getRangeMap() throws IOException {
            Configuration conf = new Configuration();
            conf.set(HBASE_ZOOKEEPER_QUORUM, HFileGeneratorParams.zkQuorum);
            HTable table = new HTable(conf, HFileGeneratorParams.hbaseTargetTable);
            Pair<byte[][], byte[][]> startEndKeys = table.getStartEndKeys();
            Map<List<String>, Integer> rangeMap = new HashMap<>();
            for (int i = 0; i < startEndKeys.getFirst().length; i++) {
                String startKey = Bytes.toString(startEndKeys.getFirst()[i]);
                String endKey = Bytes.toString(startEndKeys.getSecond()[i]);
                System.out.println("startKey = " + startKey
                        + "\tendKey = " + endKey + "\ti = " + i);
                rangeMap.put(new ArrayList<>(Arrays.asList(startKey, endKey)), i);
            }
            return rangeMap;
        }

        @Override
        public int numPartitions() {
            return numPartitions;
        }

        @Override
        public int getPartition(Object key) {
            if (key instanceof ImmutableBytesWritable) {
                try {
                    ImmutableBytesWritable immutableBytesWritableKey = (ImmutableBytesWritable) key;
                    if (rangeMap == null || rangeMap.isEmpty()) {
                        rangeMap = getRangeMap();
                    }

                    String keyString = Bytes.toString(immutableBytesWritableKey.get());
                    for (List<String> range : rangeMap.keySet()) {
                        if (keyString.compareToIgnoreCase(range.get(0)) >= 0
                                && ((keyString.compareToIgnoreCase(range.get(1)) < 0)
                                || range.get(1).equals(""))) {
                            return rangeMap.get(range);
                        }
                    }
                    logger.error("Didn't find proper key in rangeMap, so returning 0 ...");
                    return 0;
                } catch (Exception e) {
                    logger.error("When trying to get partitionID, "
                            + "encountered exception: " + e + "\t key = " + key);
                    return 0;
                }
            } else {
                logger.error("key is NOT ImmutableBytesWritable type ...");
                return 0;
            }
        }
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
            javaRdd.saveAsTextFile(TEXTFILE_PATH);

        } catch (Exception e) {
            System.out.println("Caught exception: " + e);
        }
        System.out.println("Finished saveAsTextFile to: " + TEXTFILE_PATH);
    }

    protected JavaRDD<Row> readJsonTable() {
        SparkSession.Builder builder = SparkSession.builder().appName(APP_NAME);
        if (HFileGeneratorParams.local) {
            builder.master("local");
        }

        SparkSession spark = builder.getOrCreate();

        SparkContext sparkContext = spark.sparkContext();

        // this is important. need to set the endpoint to ap-northeast-2
        sparkContext.hadoopConfiguration()
                .set("fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com");

        Dataset<Row> rows = null;
        if (!JSON_HISTORY_S3_PATHS.containsKey(HFileGeneratorParams.source)) {
            throw new RuntimeException("Unknown source : " + HFileGeneratorParams.source);
        }
        rows = spark.read().format(HFileGeneratorParams.dataSourceFormat)
                .load(JSON_HISTORY_S3_PATHS.get(HFileGeneratorParams.source) + HFileGeneratorParams.latestDateHour);

        return rows.toJavaRDD();
    }

    protected void putToHBase(JavaRDD<Row> rdd) throws IOException {
        Function2 writeToHBaseFunc = new Function2<Integer, Iterator<Row>, Iterator<Row>>() {
            @Override
            public Iterator<Row> call(Integer shardIndex, Iterator<Row> iterator)
                    throws Exception {
                put(shardIndex, iterator);
                return new ArrayList<Row>().iterator();
            }
        };

        Path tempOutputPath = Files.createTempDirectory("fishercoder")
                .toAbsolutePath();
        Files.deleteIfExists(tempOutputPath);

        try {
            rdd.repartition(HFileGeneratorParams.shards)
                    .mapPartitionsWithIndex(writeToHBaseFunc, true)
                    .saveAsTextFile(tempOutputPath.toString());
        } finally {
            FileUtils.deleteDirectory(new File(tempOutputPath.toString()));
        }
    }

    protected void put(Integer shardIndex, Iterator<Row> iterator) throws IOException {
        System.out.println("Start writing shard " + shardIndex);

        Configuration config = HBaseConfiguration.create();
        // Our json records sometimes are very big, we have
        // disable the maxsize check on the keyvalue.
        config.set("hbase.client.keyvalue.maxsize", "0");
        config.set("hbase.master", HFileGeneratorParams.hbaseMasterGateway);
        if (!HFileGeneratorParams.local) {
            config.set(HBASE_ZOOKEEPER_QUORUM, HFileGeneratorParams.zkQuorum);
        }

        Connection conn = ConnectionFactory.createConnection(config);
        Table htable = conn.getTable(TableName.valueOf(HFileGeneratorParams.hbaseTargetTable));

        while (iterator.hasNext()) {
            Row row = iterator.next();
            if (row.size() == 2) {
                String json = row.getString(1);
                if (Strings.isNullOrEmpty(json)) {
                    continue;
                }

                Put put = new Put(Bytes.toBytes(row.get(0).toString()));
                put.add(Bytes.toBytes(COL_FAMILY),
                        Bytes.toBytes(COLUMN),
                        new String(row.get(0).toString() + row.get(1).toString()).getBytes());
                htable.put(put);
            }
        }
        htable.close();
    }

    protected void sanityCheck() throws IOException {
        Configuration config = HBaseConfiguration.create();
//		config.set("hbase.master", "localhost:60000");
        if (!HFileGeneratorParams.local) {
            config.set(HBASE_ZOOKEEPER_QUORUM, HFileGeneratorParams.zkQuorum);
        }
        Connection conn = ConnectionFactory.createConnection(config);
        Table htable = conn.getTable(TableName.valueOf(HFileGeneratorParams.hbaseTargetTable));
        Get get = new Get(Bytes.toBytes("P10180816_I44714754"));
        Result result = htable.get(get);
        byte [] value = result.getValue(Bytes.toBytes(COL_FAMILY),
                Bytes.toBytes(COLUMN));
        String name = Bytes.toString(value);
        System.out.println("name = " + name);
    }
}
