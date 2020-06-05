package com.knowlegene.parent.scheduler.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @Classname ServiceOperations
 * @Description TODO
 * @Date 2020/6/5 15:27
 * @Created by limeng
 */
public final class ServiceOperations {
    private static final Log LOG = LogFactory.getLog(ServiceOperations.class);

    private ServiceOperations() {
    }


    public static void stop(Service service) {
        if (service != null) {
            service.stop();
        }
    }

    public static Exception stopQuietly(Log log, Service service) {
        try {
            stop(service);
        } catch (Exception e) {
            log.warn("When stopping the service " + service.getName()
                            + " : " + e,
                    e);
            return e;
        }
        return null;
    }


    public static Exception stopQuietly(Service service) {
        return stopQuietly(LOG, service);
    }
}
