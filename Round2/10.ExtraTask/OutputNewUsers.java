package com.jacoulin.date_2016_12_13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Jacoulin on 2016/12/19.
 */
public class OutputNewUsers {
    public static class DefMap extends Mapper<LongWritable, Text, Text, Text>{
        Text ip = new Text();
        Text type = new Text();
        String oneDay;
        String preDay;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            preDay = conf.get("preDay");
            oneDay = conf.get("oneDay");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String curDay = ((FileSplit)context.getInputSplit()).getPath().getParent().toUri().getPath();
            if (curDay.equals(oneDay)) {
                type.set("-1");
            } else if (curDay.equals(preDay)) {
                type.set("0");
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
                context.write(ip, type);
            }
        }
    }

    public static class DefReduce extends Reducer<Text, Text, Text, Text>{
        String oneDay;
        String preDay;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            oneDay = conf.get("oneDay");
            preDay = conf.get("preDay");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean exitsOneDay = false;
            boolean exitsPreDay = false;
            for (Text val : values){
                if (val.toString().equals(oneDay)) {
                    exitsOneDay = true;
                }
                if (val.toString().equals(preDay)){
                    exitsPreDay = true;
                }
            }

            if (exitsOneDay && !exitsPreDay){
                context.write(new Text(key), new Text("value"));
            }
        }
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance();

        job.setJarByClass(OutputNewUsers.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);



    }
}
