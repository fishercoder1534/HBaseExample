package com.fishercoder.hbasemr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;


/**
 * Created by stevesun on 5/11/17.
 */
public class DataImporter {
    private static final Logger LOG = LoggerFactory.getLogger(DataImporter.class);

    public static void main(String[] args) throws Exception {
        System.out.println("In here.");
        String [] pages = {"/", "/a.html", "/b.html", "/c.html"};

        Configuration config = HBaseConfiguration.create();
        config.set("hbase.master", "localhost:60000");
        Connection conn = ConnectionFactory.createConnection(config);
        Table htable = conn.getTable(TableName.valueOf("access_logs"));

        int totalRecords = 100;
        int maxID = totalRecords / 10;
        Random rand = new Random();
        System.out.println("Importing " + totalRecords + " records ....");
        for (int i=0; i < totalRecords; i++)
        {
            int userID = rand.nextInt(maxID) + 1;
            byte [] rowkey = Bytes.add(Bytes.toBytes(userID), Bytes.toBytes(i));
            String randomPage = pages[rand.nextInt(pages.length)];
            Put put = new Put(rowkey);
            put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("page"), Bytes.toBytes(randomPage));
            System.out.println("created one put.");
            htable.put(put);
            System.out.println("put one record into htable.");
        }
        htable.close();
        System.out.println("Done");
    }

}
