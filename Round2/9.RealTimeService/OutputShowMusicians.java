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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Jacoulin on 2016/12/18.
 */
public class OutputShowMusicians {
    public static class DefMap extends Mapper<LongWritable, Text, Text, Text>{
        Text date = new Text();
        Text data = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String mp = "\\d+\\.\\d+\\.\\d+\\.\\d+ " +            //请求主机
                    "[^ ]* [^ ]* " +                                   //标识符 授权用户
                    "\\[([^ ]*) [^ ]*\\] " +                             //日期时间
                    "\"[^ ]* ([^ ]*) [^ ]*\" " +                       //请求（“请求类型 请求资源 协议版本号”）
                    "\\d+ \\d+ " +                                  //状态码 传输字节数
                    "\"([^ ]*)\" " +                                 //请求来源
                    ".*";                                       //用户代理
            Pattern pattern = Pattern.compile(mp);
            Matcher m = pattern.matcher(value.toString());
            if(m.find()){
                if (isPage(m.group(2))){
                    date.set(m.group(1).substring(0, 11));
                    data.set(m.group(2));
                    context.write(date, data);
                }
            }
        }

        protected boolean isPage(String str){
            String[] request = str.split("\\?");
            if (request.length > 1) {
                if (request[0].lastIndexOf('.') == -1) {
                    return true;
                }
                return false;
            } else {
                return true;
            }
        }


    }

    public static class DefReduce extends TableReducer<Text, Text, ImmutableBytesWritable>{
        Text row_key = new Text();
        long countShow = 0L;
        long countMusician = 0L;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for(Text val : values){
                count(val.toString());
            }
        }

        protected void count(String str){
            if (!str.equals("")) {
                String[] request = str.split("\\?");
                if (request[0].lastIndexOf('.') == -1) {
                    if (!request[0].equals("/") && request[0].indexOf('/') == 0) {
                        String resReq = request[0].split("/")[1].split("-")[0];
                        if (resReq.equals("show")) {
                            countShow++;
                        } else if (resReq.equals("musicians")) {
                            countMusician++;
                        }
                    }
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            row_key.set("history");
            Put put = new Put(row_key.getBytes());
            put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("show_num"), String.valueOf(countShow).getBytes());
            put.addColumn(Bytes.toBytes("page"), Bytes.toBytes("musicians_num"), String.valueOf(countMusician).getBytes());
            context.write(new ImmutableBytesWritable(row_key.getBytes()), put);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tableName = "show_musicians";
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
        HColumnDescriptor columnDescriptor = new HColumnDescriptor("page");
        tableDescriptor.addFamily(columnDescriptor);
        admin.createTable(tableDescriptor);



        Job job = Job.getInstance(conf, "OutputShowMusicians");

        job.setJarByClass(OutputShowMusicians.class);

        job.setMapperClass(DefMap.class);
        TableMapReduceUtil.initTableReducerJob(TABLE_NAME.toString(), DefReduce.class, job);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
