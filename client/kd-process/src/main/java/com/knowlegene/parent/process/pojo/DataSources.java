package com.knowlegene.parent.process.pojo;

import lombok.Builder;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Classname DataSources
 * @Description TODO
 * @Date 2020/6/10 14:45
 * @Created by limeng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class DataSources implements Serializable {

    String className;
    String user;
    String password;
    String url;

}
