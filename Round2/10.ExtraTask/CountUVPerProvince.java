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
 * Created by Jacoulin on 2016/12/14.
 */
public class CountUVPerProvince {
    public static class DefMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        /*数据格式：ip   request country province    city*/
        Text province = new Text();
        LongWritable count = new LongWritable();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split("\\t");
            province.set(line[3]);
            count.set(1);
            context.write(province, count);
        }
    }

    public static class DefReduce extends TableReducer<Text, LongWritable, ImmutableBytesWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0L;
            for (LongWritable val : values){
                count += val.get();
            }
            Put put = new Put(key.toString().getBytes());
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("uv"), String.valueOf(count).getBytes());
            context.write(new ImmutableBytesWritable(key.toString().getBytes()), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tableName = "province_uv";
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

        Job job = Job.getInstance(conf, "CountUVPerProvince");

        job.setJarByClass(CountUVPerProvince.class);

        job.setMapperClass(DefMap.class);
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME.toString(), DefReduce.class, job);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
