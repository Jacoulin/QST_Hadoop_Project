package com.jacoulin.date_2016_12_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Jacoulin on 2016/12/13.
 */
public class RetentionRate {
    public static class DefMap extends Mapper<LongWritable, Text, Text, Text>{
        Text ip = new Text();
        Text type = new Text();

        String oneDay;
        String nextDay;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            oneDay = conf.get("oneDay");
            nextDay = conf.get("nextDay");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String path = ((FileSplit)context.getInputSplit()).getPath().getParent().toUri().getPath();

            if (path.equals(oneDay)) {
                type.set("1");
            } else if (path.equals(nextDay)) {
                type.set("2");
            }

            String mp = "(\\d+\\.\\d+\\.\\d+\\.\\d+) " +            //请求主机
                    "[^ ]* [^ ]* " +                                   //标识符 授权用户
                    "\\[([^ ]*) [^ ]*\\] " +                             //日期时间
                    "\"[^ ]* ([^ ]*) [^ ]*\" " +                       //请求（“请求类型 请求资源 协议版本号”）
                    "\\d+ \\d+ " +                                  //状态码 传输字节数
                    "\"([^ ]*)\" " +                                 //请求来源
                    ".*";                                       //用户代理
            Pattern pattern = Pattern.compile(mp);
            Matcher m = pattern.matcher(value.toString());
            if(m.find()){
                ip.set(m.group(1));
            }
            context.write(ip, type);
        }
    }

    public static class DefReduce extends Reducer<Text, Text, Text, Text>{
        static long count = 0L;
        static long num = 0L;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean exits1 = false;
            boolean exits2 = false;
            for (Text val : values){
                if (val.toString().equals("1")){
                    if (!exits1) {
                        num++;
                        exits1 = true;
                    }
                }
                if (val.toString().equals("2")){
                    exits2 = true;
                }
                if (exits1 && exits2){
                    count++;
                    break;
                }
            }

        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String rate = String.valueOf(Math.round(((count * 0.1) / (num * 0.1)) * 10000) * 0.01) + "%";
            context.write(new Text("retention rate is:"), new Text(rate));
            context.write(new Text(String.valueOf(count)), new Text(String.valueOf(num)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        conf.set("oneDay", args[0]);
        conf.set("nextDay", args[1]);
        Job job = Job.getInstance(conf, "RetentionRate");
        job.setJarByClass(RetentionRate.class);

        job.setMapperClass(DefMap.class);
        job.setReducerClass(DefReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
