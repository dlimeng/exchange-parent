package com.knowlegene.parent.process.io.file;

import org.apache.beam.sdk.values.PCollection;

import java.io.InputStream;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/12 10:07
 */
public interface HadoopFile {
    /**
     * 读取文件
     * @param path
     * @return
     */
    List<String> readFile(String path);

    /**
     * 读取文件
     * @param path
     * @return
     */
    List<String> readFileStream(InputStream path);

    /**
     * textio
     */
    PCollection<String> readFileByIO(String path);


}
