package com.knowlegene.parent.process.swap.dispatcher;

import com.knowlegene.parent.config.common.event.*;
import com.knowlegene.parent.process.swap.*;
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

        dispatcher.register(ESExportType.class,new ESExportJob.ESExportDispatcher());
        dispatcher.register(ESImportType.class,new ESImportJob.ESImportDispatcher());


        dispatcher.register(MySQLExportType.class,new MySQLExportJob.MySQLExportDispatcher());
        dispatcher.register(MySQLImportType.class,new MySQLImportJob.MySQLImportDispatcher());

        dispatcher.register(OracleExportType.class,new OracleExportJob.OracleExportDispatcher());
        dispatcher.register(OracleImportType.class,new OracleImportJob.OracleImportDispatcher());

        dispatcher.register(GbaseExportType.class,new GbaseExportJob.GbaseExportDispatcher());
        dispatcher.register(GbaseImportType.class,new GbaseImportJob.GbaseImportDispatcher());

        dispatcher.register(FileExportType.class,new FileExportJob.FileExportDispatcher());
        dispatcher.register(FileImportType.class,new FileImportJob.FileImportDispatcher());

        dispatcher.register(Neo4jExportType.class,new Neo4jExportJob.Neo4jExportDispatcher());
        dispatcher.register(Neo4jImportType.class,new Neo4jImportJob.Neo4jImportDispatcher());

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
