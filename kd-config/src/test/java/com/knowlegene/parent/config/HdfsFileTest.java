package com.knowlegene.parent.config;

import com.knowlegene.parent.config.util.HdfsFileUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * 测试hdfs
 * @Author: limeng
 * @Date: 2019/7/26 17:12
 */
public class HdfsFileTest {
    @Test
    public void getFile(){
        String filePath="D:\\sqltext.txt";
        String hdfsFilePath="hdfs://192.168.20.117:8020/testsql.txt";

        HdfsFileUtil hdfsFileUtil = new HdfsFileUtil();
        try {
            //本地文件
            List<String> content = hdfsFileUtil.getContent(filePath);
            //hdfs测试
            List<String> content1 = hdfsFileUtil.getContent(hdfsFilePath);
            Assert.assertNotNull(content);
            Assert.assertNotNull(content1);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
