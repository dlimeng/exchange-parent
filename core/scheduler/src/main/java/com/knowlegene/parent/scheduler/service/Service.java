package com.knowlegene.parent.scheduler.service;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * @Classname Service
 * @Description TODO
 * @Date 2020/6/3 13:38
 * @Created by limeng
 */
public interface Service  extends Closeable {

    /**
     * Service states
     */
    public enum STATE {

        NOTINITED(0, "NOTINITED"),

        INITED(1, "INITED"),


        STARTED(2, "STARTED"),


        STOPPED(3, "STOPPED");


        private final int value;

        private final String statename;

        private STATE(int value, String name) {
            this.value = value;
            this.statename = name;
        }

        public int getValue() {
            return value;
        }


        @Override
        public String toString() {
            return statename;
        }
    }


    void start();

    void stop();

    @Override
    void close() throws IOException;

    String getName();

    STATE  getServiceState();

    long getStartTime();

    boolean isInState(STATE state);

    Throwable getFailureCause();

    STATE getFailureState();

    boolean waitForServiceToStop(long timeout);

    public Map<String, String> getBlockers();
}
