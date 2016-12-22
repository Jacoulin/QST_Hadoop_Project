package com.jacoulin.date_2016_12_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

/**
 * Created by Jacoulin on 2016/12/19.
 */
public class RegionalDistributionOfNewUsers {
    public static class DefMap extends Mapper<LongWritable, Text, Text, Text>{
        Text province = new Text();
        Text ip = new Text();
        static long num = 0L;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\t");
            province.set(line[2]);
            ip.set(line[0]);
            num++;
            context.write(province, ip);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            conf.set("num", String.valueOf(num));
        }
    }

    public static class DefReduce extends TableReducer<Text, Text, ImmutableBytesWritable> {
        static long num = 0L;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            num = Long.parseLong(conf.get("num"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long count = 0L;
            for (Text val : values) {
                count++;
            }
            Put put = new Put(key.toString().getBytes());
            String rate = String.valueOf(Math.round(((count * 0.1) / (num * 0.1))*10000)*0.01) + "%";
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("rate"), rate.getBytes());
            context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tableName = "regional_distribution";
        TableName TABLE_NAME = TableName.valueOf(tableName);
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.zookeeper.quorum", "vm10-0-0-2.ksc.com");

        Connection conn = ConnectionFactory.createConnection(conf);
        Admin admin = conn.getAdmin();

        if(admin.tableExists(TABLE_NAME)){
            System.out.println("table exists!recreating.......");
            admin.disableTable(TABLE_NAME);
            admin.deleteTable(TABLE_NAME);
        }

        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("info");
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);

        Job job = Job.getInstance(conf, "RegionalDistributionOfNewUsers");

        job.setJarByClass(RegionalDistributionOfNewUsers.class);

        job.setMapperClass(DefMap.class);
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME.toString(), DefReduce.class, job);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
