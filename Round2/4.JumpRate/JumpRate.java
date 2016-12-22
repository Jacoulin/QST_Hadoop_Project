package com.jacoulin.date_2016_12_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
 * Created by Jacoulin on 2016/12/13.
 */
public class JumpRate {
    public static class DefMap extends Mapper<LongWritable, Text, Text, IntWritable>{
        Text jump = new Text();
        IntWritable count = new IntWritable();
        String jumpTo;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            jumpTo = conf.get("jumpTo");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String mp = "\\d+\\.\\d+\\.\\d+\\.\\d+ " +            //请求主机
                    "[^ ]* [^ ]* " +                                   //标识符 授权用户
                    "\\[([^ ]*) [^ ]*\\] " +                             //日期时间
                    "\"[^ ]+ ([^ ]+) [^ ]+\" " +                       //请求（“请求类型 请求资源 协议版本号”）
                    "\\d+ \\d+ " +                                  //状态码 传输字节数
                    "\"([^ ]+)\" " +                                 //请求来源
                    ".*";                                       //用户代理
            Pattern pattern = Pattern.compile(mp);
            Matcher m = pattern.matcher(value.toString());
            if(m.find()) {
                String str_jump = m.group(2);
                String str_refer = m.group(3);

                if (str_refer.equals("-")) {
                    if (same(str_jump, jumpTo)) {
                        jump.set(jumpTo);
                    } else {
                        jump.set(str_jump);
                    }
                    count.set(1);
                    context.write(jump, count);
                }
            }
        }

        protected boolean same(String srcReq, String tarReq) {
            if (!srcReq.equals("")) {
                String[] request = srcReq.split("\\?");
                if (request[0].lastIndexOf('.') == -1) {
                    if (!request[0].equals("/") && request[0].indexOf('/') == 0) {
                        String resReq = request[0].split("/")[1].split("-")[0];
                        if (resReq.equals(tarReq)) {
                            return true;
                        }
                    }
                }
            }
            return false;
        }
    }

    public static class DefReduce extends Reducer<Text, IntWritable, Text, Text>{
        static long countPage = 0L;
        static long countJump = 0L;
        String jumpTo;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            jumpTo = conf.get("jumpTo");
        }

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values){
                if (key.toString().equals(jumpTo)) {
                    countJump++;
                }
                countPage++;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            String rate = String.valueOf(Math.round(((countJump * 0.1) / (countPage * 0.1)) * 10000) * 0.01) + "%";
            context.write(new Text("rate of " + jumpTo + " is :"), new Text(rate));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("jumpTo", args[2]);
        Job job = Job.getInstance(conf, "JumpRate");

        job.setJarByClass(JumpRate.class);
        job.setMapperClass(DefMap.class);
        job.setReducerClass(DefReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
