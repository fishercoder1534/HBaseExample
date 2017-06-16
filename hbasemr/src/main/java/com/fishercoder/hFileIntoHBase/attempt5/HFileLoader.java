package com.fishercoder.hFileIntoHBase.attempt5;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * Created by stevesun on 6/7/17.
 */
public class HFileLoader {
    public static void main(String[] args) throws Exception {
        Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", "10.211.126.134,10.211.126.103,10.211.126.130");
        hbaseConf.set("hbase.table.name", "steve1");
        HTable table = new HTable(hbaseConf, "steve1");
        System.out.println("table.getName() = " + table.getName());

        Configuration conf = new Configuration();
        LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);
        loader.doBulkLoad(new Path("HFILE_PATH"), table);
    }
}
