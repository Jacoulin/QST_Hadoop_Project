package com.jacoulin.date_2016_12_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Jacoulin on 2016/12/14.
 */
public class CountUVFromAndroidOrIOS {
    public static class DefMap extends Mapper<LongWritable, Text, Text, Text> {
        Text ip = new Text();
        Text type = new Text();
        LongWritable count = new LongWritable();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String mp = "(\\d+\\.\\d+\\.\\d+\\.\\d+) " +            //请求主机
                    "[^ ]* [^ ]* " +                                   //标识符 授权用户
                    "\\[[^ ]* [^ ]*\\] " +                             //日期时间
                    "\"[^ ]+ [^ ]+ [^ ]+\" " +                       //请求（“请求类型 请求资源 协议版本号”）
                    "\\d+ \\d+ " +                                  //状态码 传输字节数
                    "\"[^ ]+\" " +                                 //请求来源
                    "\".*((Android|iOS)).*\"";                                       //用户代理
            Pattern pattern = Pattern.compile(mp);
            Matcher m = pattern.matcher(value.toString());
            if(m.find()){
                String strIP = m.group(1);
                String strAgency = m.group(2);
                String strType = strAgency.toLowerCase();
                if (strType.equals("android")) {
                    type.set("android");
                }
                if (strType.equals("ios")) {
                    type.set("ios");
                }
                ip.set(strIP);
                context.write(ip, type);
            }
        }
    }

    public static class DefReduce extends Reducer<Text, Text, Text, LongWritable> {
        static long countIOS = 0L;
        static long countAndroid = 0L;
        boolean typeOfAndroid = false;
        boolean typeOfIOS = false;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                String type = val.toString();
                if (type.equals("android")) {
                    typeOfAndroid = true;
                }
                if (type.equals("ios")) {
                    typeOfIOS = true;
                }
                if (typeOfAndroid && typeOfIOS){
                    break;
                }
            }

            if (typeOfAndroid) {
                countAndroid++;
            }
            if (typeOfIOS) {
                countIOS++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(new Text("UV of Android is :"), new LongWritable(countAndroid));
            context.write(new Text("UV of IOS is :"), new LongWritable(countIOS));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CountUVFromAndroidOrIOS");
        job.setJarByClass(CountUVFromAndroidOrIOS.class);

        job.setMapperClass(DefMap.class);
        job.setReducerClass(DefReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
