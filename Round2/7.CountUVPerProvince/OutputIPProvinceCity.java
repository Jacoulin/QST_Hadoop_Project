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
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Jacoulin on 2016/12/14.
 */
public class OutputIPProvinceCity {
    public static class DefMap extends Mapper<LongWritable, Text, Text, Text>{
        Text ip = new Text();
        Text address = new Text();
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String mp = "(\\d+\\.\\d+\\.\\d+\\.\\d+) " +            //请求主机
                    "[^ ]* [^ ]* " +                                   //标识符 授权用户
                    "\\[([^ ]*) [^ ]*\\] " +                             //日期时间
                    "\"[^ ]* ([^ ]*) [^ ]*\" " +                       //请求（“请求类型 请求资源 协议版本号”）
                    "\\d+ \\d+ " +                                  //状态码 传输字节数
                    "\"([^ ]*)\"" +                                 //请求来源
                    ".*";                                       //用户代理
            Pattern pattern = Pattern.compile(mp);
            Matcher matcher = pattern.matcher(value.toString());
            if (matcher.find()) {
                String strIP = matcher.group(1);
                String strAddress = getAddressByIP(strIP);
                ip.set(strIP);
                address.set(strAddress);
                context.write(ip, address);
            }
        }

        protected String getAddressByIP(String ip) throws IOException {
            URL url = new URL( "http://ip.taobao.com/service/getIpInfo.php?ip=" + ip );
            URLConnection conn = url.openConnection();
            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream(), "GBK"));
            String line = null;
            StringBuffer result = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                result.append(line);
            }
            reader.close();
            String ori_json = result.toString();
            JSONObject json = null;
            JSONObject address_json = null;
            String province = "null";
            String city = "null";
            try {
                json = new JSONObject(ori_json);
                address_json = new JSONObject(json.getString("data"));
                province = address_json.getString("region");
                city = address_json.getString("city");
            } catch (JSONException e) {
                e.printStackTrace();
            }
            String address = province + "\\t" + city;
            return address;
        }
    }

    public static class DefReduce extends Reducer<Text, Text, Text, Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> itr = values.iterator();
            if (itr.hasNext()){
                context.write(new Text(key), new Text(itr.next()));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "OutputIPProvinceCity");
        job.setJarByClass(OutputIPProvinceCity.class);

        job.setMapperClass(DefMap.class);
        job.setReducerClass(DefReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
