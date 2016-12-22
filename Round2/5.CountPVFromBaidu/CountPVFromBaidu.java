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
 * Created by Jacoulin on 2016/12/13.
 */
public class CountPVFromBaidu {
    public static class DefMap extends Mapper<LongWritable, Text, Text, LongWritable> {
        Text date = new Text();
        String refer;
        String referFromBaidu;
        LongWritable count = new LongWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            referFromBaidu = conf.get("referFromBaidu");
        }

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
                String[] refers = m.group(3).split("/+");
                if (refers.length > 1) {
                    refer = refers[1].substring(refers[1].indexOf('.') + 1);
                } else {
                    refer = refers[0];
                }
                if (refer.equals(referFromBaidu)) {
                    if (isPage(m.group(2))){
                        date.set(m.group(1).substring(0, 11));
                        count.set(1);
                        context.write(date, count);
                    }
                }
            }
        }

        protected boolean isPage(String str){
            if (!str.equals("")) {
                String[] request = str.split("\\?");
                if (request.length > 1) {
                    if (request[0].lastIndexOf('.') == -1) {
                        return true;
                    }
                    return false;
                } else if (request[0].equals("/")){
                    return true;
                }
            }
            return false;
        }
    }

    public static class DefReduce extends Reducer<Text, LongWritable, Text, LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable val : values){
                count += val.get();
            }
            context.write(new Text(key), new LongWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("referFromBaidu", "baidu.com");
        Job job = Job.getInstance(conf, "CountPVFromBaidu");

        job.setJarByClass(CountPVFromBaidu.class);
        job.setMapperClass(DefMap.class);
        job.setReducerClass(DefReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
