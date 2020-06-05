package com.knowlegene.parent.scheduler.service;

/**
 * @Classname TestSimpleMRAppMaster2
 * @Description TODO
 * @Date 2020/6/5 16:44
 * @Created by limeng
 */
public class TestSimpleMRAppMasterRun {
    public static void main(String[] args)  throws Exception  {
        String jobID = "job_2020";
        TestSimpleMRAppMaster appMaster  = new TestSimpleMRAppMaster("Simple MRAppMaster", jobID, 5);

        appMaster.serviceInit();
        appMaster.serviceStart();
        /**
         * Receive JOB_INIT event, scheduling tasks
         * Receive JOB_KILL event, killing all the tasks
         * Receive T_SCHEDULE of taskjob_2020_task_0
         * Receive T_SCHEDULE of taskjob_2020_task_1
         * Receive T_SCHEDULE of taskjob_2020_task_2
         * Receive T_SCHEDULE of taskjob_2020_task_3
         * Receive T_SCHEDULE of taskjob_2020_task_4
         * Receive T_KILL event of taskjob_2020_task_0
         * Receive T_KILL event of taskjob_2020_task_1
         * Receive T_KILL event of taskjob_2020_task_2
         * Receive T_KILL event of taskjob_2020_task_3
         * Receive T_KILL event of taskjob_2020_task_4
         */

        appMaster.getDispatcher().getEventHandler().handle(new TestJobEvent(jobID, TestJobEventType.JOB_INIT));
        appMaster.getDispatcher().getEventHandler().handle(new TestJobEvent(jobID, TestJobEventType.JOB_KILL));


    }
}
