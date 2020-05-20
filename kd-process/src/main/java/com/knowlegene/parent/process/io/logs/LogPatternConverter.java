package com.knowlegene.parent.process.io.logs;

import com.knowlegene.parent.config.util.BaseUtil;
import com.knowlegene.parent.process.util.JobThreadLocalUtil;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;
import org.apache.logging.log4j.core.pattern.PatternConverter;
import org.apache.logging.log4j.message.Message;

/**
 * 日志标记
 * @Author: limeng
 * @Date: 2019/8/13 17:55
 */
@Plugin(name = "LogPatternConverter", category = PatternConverter.CATEGORY)
@ConverterKeys({ "H" })
public class LogPatternConverter extends LogEventPatternConverter {

    private static final LogPatternConverter INSTANCE = new LogPatternConverter();

    public static LogPatternConverter newInstance(final String[] options) {
        return INSTANCE;
    }

    private LogPatternConverter(){
        super("LogId","logId");
    }

    @Override
    public void format(LogEvent logEvent, StringBuilder stringBuilder) {
        final Message msg = logEvent.getMessage();
        String result = null;
        if (msg != null) {
            //String result = msg.getFormattedMessage();
            result = getPayplusLogUUID() ;
            stringBuilder.append(result);
        }
    }

    /**
     * 业务日志全局jobid
     * @return
     */
    protected  String getPayplusLogUUID() {
        StringBuilder builder = new StringBuilder();
        String job = JobThreadLocalUtil.getJob();
        if(BaseUtil.isNotBlank(job)){
            builder.append("JobId(");
            builder.append(job);
            builder.append(")");
        }
        return builder.toString();
    }
}
