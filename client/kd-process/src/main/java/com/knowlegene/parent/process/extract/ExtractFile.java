package com.knowlegene.parent.process.extract;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.io.file.HadoopFile;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.annotation.Resource;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/12 19:08
 */
public class ExtractFile {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Resource
    private HadoopFile hadoopFile;

    /**
     * 读文件绝对路径
     * @param path
     * @return
     */
    public List<String> readAbsolutePath(String path){
        if(BaseUtil.isNotBlank(path)){
            try {
                String newPath = URLDecoder.decode(path, "utf-8");
                return hadoopFile.readFile(newPath);
            } catch (UnsupportedEncodingException e) {
                logger.error("readAbsolutePath is error=》msg:{},path:{}",e.getMessage(),path);
            }
        }
       return null;
    }

    /**
     * 读资源resource文件夹流
     * @param path
     * @return
     */
    public List<String> readStreamPath(String path){
        if(BaseUtil.isNotBlank(path)){
            InputStream in = this.getClass().getResourceAsStream(path);
            return this.hadoopFile.readFileStream(in);
        }
        return null;
    }

    public PCollection<String> readFile(String path){
        return hadoopFile.readFileByIO(path);
    }
}
