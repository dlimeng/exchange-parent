package com.knowlegene.parent.scheduler.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.Map;

/**
 * @Classname CompositeService
 * @Description TODO
 * @Date 2020/6/4 19:38
 * @Created by limeng
 */
public class CompositeService extends AbstractService {

    private static final Log LOG = LogFactory.getLog(CompositeService.class);

    public CompositeService(String name) {
        super(name);
    }

    protected static final boolean STOP_ONLY_STARTED_SERVICES = false;


    @Override
    public void close() throws IOException {

    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public long getStartTime() {
        return 0;
    }

    @Override
    public Map<String, String> getBlockers() {
        return null;
    }
}
