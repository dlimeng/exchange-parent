package com.knowlegene.parent.process.swap.dispatcher;

import com.knowlegene.parent.config.common.event.HiveExportType;
import com.knowlegene.parent.config.common.event.HiveImportType;
import com.knowlegene.parent.config.common.event.SwapJobEventType;
import com.knowlegene.parent.process.swap.HiveExportJob;
import com.knowlegene.parent.process.swap.HiveImportJob;
import com.knowlegene.parent.process.swap.JobBase;
import com.knowlegene.parent.process.util.SwapMasterUtil;
import com.knowlegene.parent.scheduler.event.AsyncDispatcher;
import com.knowlegene.parent.scheduler.event.Dispatcher;
import com.knowlegene.parent.scheduler.service.CompositeService;
import com.knowlegene.parent.scheduler.service.Service;

/**
 * @Classname SwapMaster
 * @Description TODO
 * @Date 2020/6/11 15:58
 * @Created by limeng
 */
public class SwapMaster extends CompositeService {
    private Dispatcher dispatcher;//中央异步调度器

    private volatile boolean isAlive=true;

    public void setAlive(boolean alive) {
        isAlive = alive;
    }

    public boolean isAlive() {
        return isAlive;
    }

    public SwapMaster(String name) {
        super(name);
        SwapMasterUtil.getInstance(this);
    }

    @Override
    public void serviceInit() throws Exception {
        dispatcher = new AsyncDispatcher();

        //注册Job和Task事件调度器
        dispatcher.register(SwapJobEventType.class,new JobBase.SwapJobEventDispatcher());
        dispatcher.register(HiveExportType.class,new HiveExportJob.HiveExportDispatcher());
        dispatcher.register(HiveImportType.class,new HiveImportJob.HiveImportDispatcher());
        addService((Service)dispatcher);
        super.serviceInit();
    }

    @Override
    public void serviceStart() throws Exception {
        super.serviceStart();
    }

    @Override
    public void serviceStop() throws Exception {
        super.serviceStop();
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }
}
