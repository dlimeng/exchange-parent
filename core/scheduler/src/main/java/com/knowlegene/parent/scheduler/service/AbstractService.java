package com.knowlegene.parent.scheduler.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @Classname AbstractService
 * @Description TODO
 * @Date 2020/6/3 13:42
 * @Created by limeng
 */
public abstract class AbstractService  implements Service{

    private static final Log LOG = LogFactory.getLog(AbstractService.class);

    private final String name;

    private volatile ServiceStateModel stateModel;

    private long startTime;

    private Exception failureCause;

    private volatile STATE failureState = null;

    private final AtomicBoolean terminationNotification = new AtomicBoolean(false);

    private final Object stateChangeLock = new Object();

    public AbstractService(String name) {
        this.name = name;
        this.stateModel = new ServiceStateModel(name);
    }

    @Override
    public  synchronized STATE getServiceState() {
        return stateModel.getState();
    }

    @Override
    public final synchronized Throwable getFailureCause() {
        return failureCause;
    }

    @Override
    public synchronized STATE getFailureState() {
        return failureState;
    }


    @Override
    public void init() {
        if (isInState(STATE.INITED)) {
            return;
        }
        synchronized (stateChangeLock) {
            if (enterState(STATE.INITED) != STATE.INITED) {
                try {
                    serviceInit();
                }catch (Exception e) {
                    noteFailure(e);
                    ServiceOperations.stopQuietly(LOG, this);
                    throw ServiceStateException.convert(e);
                }

            }
        }
    }

    @Override
    public void stop(){
        if(isInState(STATE.STOPPED)){
            return;
        }

        synchronized (stateChangeLock){
            if(enterState(STATE.STOPPED) != STATE.STOPPED){
                try {
                    serviceStop();
                }catch (Exception e){
                    noteFailure(e);
                    throw ServiceStateException.convert(e);
                }finally {
                    terminationNotification.set(true);
                    synchronized (terminationNotification) {
                        terminationNotification.notifyAll();
                    }
                }

            }else{
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Ignoring re-entrant call to stop()");
                }
            }
        }

    }


    @Override
    public void start(){
        if(isInState(STATE.STARTED)){
            return;
        }
        //enter the started state
        synchronized (stateChangeLock){
            if(stateModel.enterState(STATE.STARTED) != STATE.STARTED){
                try {
                  startTime = System.currentTimeMillis();
                  serviceStart();

                  if(isInState(STATE.STARTED)){
                      if(LOG.isDebugEnabled()){
                          LOG.debug("Service " + getName() + " is started");
                      }
                  }

                }catch (Exception e){
                    noteFailure(e);
                    ServiceOperations.stopQuietly(LOG, this);
                    throw ServiceStateException.convert(e);
                }
            }
        }
    }





    @Override
    public final boolean waitForServiceToStop(long timeout){
        boolean completed = terminationNotification.get();

        while (!completed){
            try {
                synchronized(terminationNotification) {
                    terminationNotification.wait(timeout);
                }
                completed = true;

            }catch (InterruptedException e){
                completed = terminationNotification.get();
            }
        }
        return terminationNotification.get();
    }


    protected void serviceStart() throws Exception {

    }

    protected void serviceStop() throws Exception {

    }

    private STATE enterState(STATE newState){
        assert stateModel != null : "null state in " + name + " " + this.getClass();
        STATE oldState = stateModel.enterState(newState);
        if(oldState != newState){
            if(LOG.isDebugEnabled()){
                LOG.debug(
                        "Service: " + getName() + " entered state " + getServiceState());
            }
        }
        return oldState;
    }

    @Override
    public final boolean isInState(Service.STATE expected){
        return stateModel.isInState(expected);
    }

    @Override
    public String toString() {
        return "Service " + name + " in state " + stateModel;
    }

    protected final void noteFailure(Exception exception){
        if(LOG.isDebugEnabled()){
            LOG.debug("noteFailure " + exception, null);
        }

        if(exception == null){
            return;
        }

        synchronized (this){
            if(failureCause == null){
                failureCause = exception;
                failureState = getServiceState();
                LOG.info("Service " + getName()
                                + " failed in state " + failureState
                                + "; cause: " + exception,
                        exception);
            }
        }
    }

    @Override
    public final void close() throws IOException {
        stop();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public long getStartTime() {
        return startTime;
    }


    protected void serviceInit() throws Exception {

    }

}
