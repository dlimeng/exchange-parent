package com.knowlegene.parent.process.pojo.file;

import com.knowlegene.parent.process.common.annotation.StoredAsProperty;
import lombok.Data;

import java.io.Serializable;

/**
 * @Classname FileOptions
 * @Description TODO
 * @Date 2020/6/23 14:10
 * @Created by limeng
 */
@Data
public class FileOptions implements Serializable {
    @StoredAsProperty("file.url")
    private String filePath;
    @StoredAsProperty("field.delim")
    private String fieldDelim;
    @StoredAsProperty("field.fieldTitle")
    private String[] fieldTitle;
}
