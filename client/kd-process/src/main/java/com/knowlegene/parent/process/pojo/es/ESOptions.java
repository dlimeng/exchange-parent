package com.knowlegene.parent.process.pojo.es;

import com.knowlegene.parent.process.common.annotation.StoredAsProperty;
import com.knowlegene.parent.process.pojo.NestingFields;
import lombok.Data;

import java.io.Serializable;

/**
 * @Classname ESOptions
 * @Description TODO
 * @Date 2020/6/23 11:07
 * @Created by limeng
 */
@Data
public class ESOptions implements Serializable {
    @StoredAsProperty("es.addrs")
    private String[] esAddrs;
    @StoredAsProperty("es.index")
    private String esIndex;
    @StoredAsProperty("es.type")
    private String esType;
    @StoredAsProperty("es.query")
    private String esQuery;
    @StoredAsProperty("es.idFn")
    private String esIdFn;

    private NestingFields nestingFields;
}
