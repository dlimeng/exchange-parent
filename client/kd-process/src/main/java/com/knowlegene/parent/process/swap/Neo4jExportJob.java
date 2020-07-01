package com.knowlegene.parent.process.swap;

import com.knowlegene.parent.config.common.constantenum.DBOperationEnum;
import com.knowlegene.parent.config.common.event.Neo4jExportType;
import com.knowlegene.parent.process.pojo.SwapOptions;
import com.knowlegene.parent.process.pojo.neo4j.Neo4jOptions;
import com.knowlegene.parent.process.swap.event.Neo4jExportTaskEvent;
import com.knowlegene.parent.scheduler.event.EventHandler;
import com.knowlegene.parent.scheduler.utils.CacheManager;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;


/**
 * @Author: limeng
 * @Date: 2019/9/23 17:46
 */
public class Neo4jExportJob extends ExportJobBase  {
    private static Neo4jOptions neo4jOptions;

    public Neo4jExportJob() {
    }

    public Neo4jExportJob(SwapOptions options) {
        super(options);
    }

    private static Neo4jOptions getDbOptions(){
        if(neo4jOptions == null){
            String name = DBOperationEnum.NEO4J_EXPORT.getName();
            Object options = getOptions(name);
            if(options != null){
                neo4jOptions = (Neo4jOptions)options;
            }
        }
        return neo4jOptions;
    }



    public static PCollection<Row> query() {
        return null;
    }


    public static class Neo4jExportDispatcher implements EventHandler<Neo4jExportTaskEvent> {
        @Override
        public void handle(Neo4jExportTaskEvent event) {
            if (event.getType() == Neo4jExportType.T_EXPORT) {
                getLogger().info("Neo4jExportDispatcher is start");

                PCollection<Row> rows = query();
                CacheManager.setCache(DBOperationEnum.PCOLLECTION_QUERYS.getName(), rows);
            }
        }
    }
}
