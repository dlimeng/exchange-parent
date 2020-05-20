package com.knowlegene.parent.process.extract;


import javax.annotation.Resource;
import java.io.Serializable;

/**
 * @Author: limeng
 * @Date: 2019/8/12 10:40
 */
public class BaseExtract implements Serializable {

    public BaseExtract() {
        this.extractHive = new ExtractHive();

        this.extractFile = new ExtractFile();
    }
    @Resource
    private ExtractHive extractHive;
    @Resource
    private ExtractFile extractFile;

    public ExtractHive getExtractHive() {
        return extractHive;
    }

    public ExtractFile getExtractFile() {
        return extractFile;
    }



}
