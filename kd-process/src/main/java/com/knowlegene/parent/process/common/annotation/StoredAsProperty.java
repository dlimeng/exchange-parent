package com.knowlegene.parent.process.common.annotation;

import java.lang.annotation.*;

/**
 * @Author: limeng
 * @Date: 2019/8/20 14:03
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface StoredAsProperty {
    String value();
}
