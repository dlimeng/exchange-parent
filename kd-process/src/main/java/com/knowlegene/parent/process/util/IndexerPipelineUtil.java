package com.knowlegene.parent.process.util;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.runners.options.IndexerPipelineOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 检验参数
 * @Author: limeng
 * @Date: 2019/7/23 22:09
 */
public class IndexerPipelineUtil {
    private static Logger logger = LoggerFactory.getLogger(IndexerPipelineUtil.class);

    public static void validateIndexerPipelineOptions(IndexerPipelineOptions option) throws Exception{
        int numSourceTypes = 0;
        if(BaseUtil.isNotBlank(option.getPattern())){
            numSourceTypes ++;
        }
        if (numSourceTypes == 0) {
            throw new IllegalArgumentException("没有选择 Pattern");
        }

        Boolean sqlFile = option.isSqlFile();
        String filePath = option.getFilePath();
        if(sqlFile){
            if(BaseUtil.isBlank(filePath)){
                throw new IllegalArgumentException("读取文件sql缺失sqlFile/filePath");
            }
        }
    }

}
