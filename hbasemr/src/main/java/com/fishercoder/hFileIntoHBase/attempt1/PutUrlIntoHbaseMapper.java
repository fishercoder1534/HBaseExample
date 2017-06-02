package com.fishercoder.hFileIntoHBase.attempt1;

/**
 * Created by stevesun on 6/1/17.
 */
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PutUrlIntoHbaseMapper
        extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

    public static String toHexString(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();

        for (int i = 0; i < bytes.length; i++) {
            String hex = Integer.toHexString(0xFF & bytes[i]);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }

        return hexString.toString();
    }

    private String md5(String s) throws NoSuchAlgorithmException {
        byte b[] = MessageDigest.getInstance("MD5").digest(s.getBytes());
        return toHexString(b);
    }

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (value == null) {
            context.getCounter("user_mapper", "TOTAL").increment(1L);
            return;
        }

        try {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length != 8) {
                context.getCounter("user_mapper", "INVALID").increment(1L);
                return;
            }

            byte[] sitemap_id           = Bytes.toBytes(parts[0].trim()); // top level sitemap id
            byte[] top_level_sitemap    = Bytes.toBytes(parts[1].trim());
            byte[] second_level_sitemap = Bytes.toBytes(parts[2].trim());
            byte[] url                  = Bytes.toBytes(parts[3].trim());
            byte[] parse_time           = Bytes.toBytes(parts[4].trim());
            byte[] sitemap_type         = Bytes.toBytes(parts[7].trim());
            byte[] k                    = Bytes.toBytes(md5(parts[3].trim()+"#_#!"+parts[7]) ); // key for hbase
            long r_timestamp = 5000000000000L - System.currentTimeMillis();

            Put put = new Put(k, r_timestamp); // key : url_sitemapType
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("url"), url);
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("smap_id"), sitemap_id);
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("smap_type"), sitemap_type);
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("top_smap"), top_level_sitemap);
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("second_smap"), second_level_sitemap);
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("first_parse_time"), parse_time);
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("select_ts"), Bytes.toBytes("0")); // default does not select
            put.addImmutable(Bytes.toBytes("i"), Bytes.toBytes("fetch_ts"), Bytes.toBytes("0")); // default does not fetch

            context.write(new ImmutableBytesWritable(k), put);
        }
        catch(Exception e) {
            context.getCounter("user_mapper", "EXCEPTION").increment(1L);
        }

    }

}
