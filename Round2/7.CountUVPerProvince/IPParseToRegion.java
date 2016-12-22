package com.jacoulin.date_2016_12_13;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;

/**
 * Created by Jacoulin on 2016/12/21.
 */
public class IPParseToRegion {
    public static void main(String[] args) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader("ip.txt"));
        BufferedWriter bw = new BufferedWriter(new FileWriter("ip_times_province"));
        String line = null;
        while((line=br.readLine())!=null){
            String[] record = line.split("\\t");
            if (record.length > 0){
                String strIP = record[0];
                String address = getAddressByIP(strIP);
                bw.write(line + "\t" + address);
                bw.newLine();
            }
        }
        br.close();
        bw.close();
    }

    public static String getAddressByIP(String ip) throws IOException {
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
        String country = "null";
        String province = "null";
        String city = "null";
        try {
            json = new JSONObject(ori_json);
            address_json = new JSONObject(json.getString("data"));
            country = address_json.getString("country");
            province = address_json.getString("region");
            city = address_json.getString("city");
        } catch (JSONException e) {
            e.printStackTrace();
        }
        String address = country + "\t" + province + "\t" + city;
        return address;
    }
}
