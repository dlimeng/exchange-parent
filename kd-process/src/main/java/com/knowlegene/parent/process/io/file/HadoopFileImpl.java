package com.knowlegene.parent.process.io.file;

import com.knowlegene.parent.config.util.HdfsFileUtil;
import com.knowlegene.parent.config.util.HdfsPipelineUtil;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

/**
 * @Author: limeng
 * @Date: 2019/8/12 10:07
 */
public class HadoopFileImpl implements HadoopFile {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    @Override
    public List<String> readFile(String path) {
        HdfsFileUtil hdfsFileUtil = new HdfsFileUtil();
        try {
            List<String> content = hdfsFileUtil.getContent(path);
            List<String> conversions = hdfsFileUtil.conversionSet(content);
            return conversions;
        } catch (IOException e) {
            logger.error("readFiles=>msg:{}",e.getMessage());
        }
        return null;
    }

    @Override
    public List<String> readFileStream(InputStream path) {
        HdfsFileUtil hdfsFileUtil = new HdfsFileUtil();
        try {
            List<String> content = hdfsFileUtil.getContentStream(path);
            List<String> conversions = hdfsFileUtil.conversionSet(content);
            return conversions;
        } catch (IOException e) {
            logger.error("readFileStream=>msg:{}",e.getMessage());
        }
        return null;
    }

    @Override
    public PCollection<String> readFileByIO(String path) {
        return HdfsPipelineUtil.startDocumentImport(path);
    }
}
