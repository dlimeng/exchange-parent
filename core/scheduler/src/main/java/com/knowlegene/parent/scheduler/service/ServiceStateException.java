package com.knowlegene.parent.scheduler.service;

/**
 * @Classname ServiceStateException
 * @Description TODO
 * @Date 2020/6/3 14:15
 * @Created by limeng
 */
public class ServiceStateException extends RuntimeException {

    private static final long serialVersionUID = -4819737863134593472L;

    public ServiceStateException(String message) {
        super(message);
    }

    public ServiceStateException(String message, Throwable cause) {
        super(message, cause);
    }

    public ServiceStateException(Throwable cause) {
        super(cause);
    }

    public static RuntimeException convert(Throwable fault) {
        if (fault instanceof RuntimeException) {
            return (RuntimeException) fault;
        } else {
            return new ServiceStateException(fault);
        }
    }

    public static RuntimeException convert(String text, Throwable fault) {
        if (fault instanceof RuntimeException) {
            return (RuntimeException) fault;
        } else {
            return new ServiceStateException(text, fault);
        }
    }
}
