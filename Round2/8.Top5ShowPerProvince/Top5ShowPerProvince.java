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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Jacoulin on 2016/12/18.
 */
public class Top5ShowPerProvince {

    public static class Page{
        String page;
        long num;

        public Page() {
        }

        public String getPage() {
            return page;
        }

        public long getNum() {
            return num;
        }

        public void set(String page, long num){
            this.page = page;
            this.num = num;
        }
    }

    public static class DefMap extends Mapper<LongWritable, Text, Text, Text> {
        Text province = new Text();
        Text show_num = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            /*
            数据格式：show   num   province    city
            */
            String[] line = value.toString().split("\\t");
            province.set(line[2]);
            show_num.set(line[0] + "\t" + line[1]);
            context.write(province, show_num);
        }
    }

    public static class DefReduce extends Reducer<Text, Text, Text, Text>{
        List<Page> list = new ArrayList<>();
        Text show = new Text();
        Long num;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Page page = new Page();
            for (Text val : values){
                String[] line = val.toString().split("\\t");
                if (list.size() < 5){
                    page = new Page();
                    page.set(line[0], Long.parseLong(line[1]));
                    list.add(page);
                } else {
                    page.set(line[0], Long.parseLong(line[1]));
                    if (page.getNum() > list.get(4).getNum()){
                        list.get(4).set(page.getPage(), page.getNum());
                    }
                }
                Collections.sort(list, new Comparator<Page>() {
                    @Override
                    public int compare(Page o1, Page o2) {
                        return (int)(o2.getNum() - o1.getNum());
                    }
                });
            }
            for (Page d : list){
                context.write(key, new Text(d.getPage() + "\t" + String.valueOf(d.getNum())));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Top5ShowPerProvince");

        job.setJarByClass(Top5ShowPerProvince.class);

        job.setMapperClass(DefMap.class);
        job.setReducerClass(DefReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
