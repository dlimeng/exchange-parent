package com.knowlegene.parent.config.util;

import org.apache.beam.sdk.values.KV;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * hdfs file
 * @Author: limeng
 * @Date: 2019/7/25 21:58
 */
public class HdfsFileUtil {

    private Configuration conf;
    private static String REX=";";
    //注释标记
    private String comment = "^[#\\-\\-]";
    public HdfsFileUtil() {

    }

    /**
     * 读取文件内容
     */
    public  List<String> getContent(String remoteFilePath) throws IOException {
        List<String> result = null;
        this.getConf(remoteFilePath);
        FileSystem fs = FileSystem.get(conf);
        Path remotePath = new Path(remoteFilePath);
        FSDataInputStream in = fs.open(remotePath);
        result = this.getContentStream(in);
        in.close();
        fs.close();
        return result;
    }

    /**
     * 读取文件内容
     */
    public  List<String> getContentStream(InputStream remoteFilePath) throws IOException {
        List<String> result = new ArrayList<>();
        BufferedReader d = new BufferedReader(new InputStreamReader(remoteFilePath));
        String line = null;
        Pattern p = Pattern.compile(comment);
        Matcher m=null;
        while ((line = d.readLine()) != null) {
            m = p.matcher(line);
            if(!m.find()){
                result.add(line);
            }
        }
        d.close();
        return result;
    }

    public void getConf(String remoteFilePath){
        conf = new Configuration();
        Boolean hadoopPath = HdfsPipelineUtil.isHadoopPath(remoteFilePath);
        if(hadoopPath){
            Properties properties = PropertiesUtil.getProperties();
            String defaultName = properties.getProperty("kd.hdfs.defaultname");
            conf = new Configuration();
            conf.set("fs.default.name", defaultName);
        }else{
            conf.set("fs.default.name", "file:///");
            conf.set("mapred.job.tracker", "local");
        }
    }

    /**
     * 清楚格式问题
     * 转换集合
     * @param sqlList
     * @return
     */
    public List<String> conversionSet(List<String> sqlList) {
        List<String> result = null;
        if(BaseUtil.isBlankSet(sqlList)){
            return result;
        }
        result = new ArrayList<>();
        StringBuffer sb= new StringBuffer();
        for (String sql:sqlList){
            sb.append(sql);
            if(sql.endsWith(REX)){
                String newSql=sb.toString().replaceAll(";","");
                result.add(newSql);
                sb= new StringBuffer();
            }
        }
        return result;
    }


    public static KV<String,String> splitMark(String str,String rex){
        String[] split = str.split(rex);
        if(split != null && split.length>1){
            return KV.of(split[0],"."+split[1]);
        }
        return KV.of(str,".cvs");
    }

}
