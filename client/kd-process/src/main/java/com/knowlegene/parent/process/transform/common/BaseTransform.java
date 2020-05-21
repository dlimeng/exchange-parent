package com.knowlegene.parent.process.transform.common;


import com.knowlegene.parent.process.transform.HiveFun;

import javax.annotation.Resource;

/**
 * @Author: limeng
 * @Date: 2019/8/28 17:24
 */

public class BaseTransform {

    public BaseTransform() {
        this.hiveFun = new HiveFun();
    }

    @Resource
    private HiveFun hiveFun;

    public HiveFun getHiveFun() {
        return hiveFun;
    }
}
