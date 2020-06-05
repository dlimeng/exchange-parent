package com.knowlegene.parent.scheduler.service;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;

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

    private final List<Service>  serviceList = new ArrayList<>();


    public List<Service> getServices(){
        synchronized (serviceList){
            return new ArrayList<>(serviceList);
        }
    }

    protected void addService(Service service){
        if(LOG.isDebugEnabled()){
            LOG.debug("Adding service " + service.getName());
        }

        synchronized (serviceList){
            serviceList.add(service);
        }
    }

    protected boolean addIfService(Object object){
       if(object instanceof Service){
            addService((Service) object);
            return true;
       }else{
           return false;
       }
    }

    protected synchronized boolean removeService(Service service){
        synchronized (serviceList){
            return serviceList.remove(service);
        }
    }

    @Override
    protected void serviceStart() throws Exception {
        List<Service> services = getServices();
        if(LOG.isDebugEnabled()){
            LOG.debug(getName() + ": starting services, size=" + services.size());
        }
        for (Service service : services){
            service.start();
        }
        super.serviceStart();
    }

    @Override
    protected void serviceStop() throws Exception  {
        int numOfServicesToStop = serviceList.size();
        if(LOG.isDebugEnabled()){
            LOG.debug(getName() + ": stopping services, size=" + numOfServicesToStop);
        }
        stop(numOfServicesToStop,STOP_ONLY_STARTED_SERVICES);
        super.serviceStop();
    }



    private void stop(int numOfServicesStarted, boolean stopOnlyStartedServices) {
        Exception firstException = null;
        List<Service> services = getServices();
        for (int i = numOfServicesStarted -1 ; i >= 0 ; i--) {
            Service service = services.get(i);
            if(LOG.isDebugEnabled()){
                LOG.debug("Stopping service #" + i + ": " + service);
            }

            STATE state = service.getServiceState();

            if(state == STATE.STARTED || (!stopOnlyStartedServices && state == STATE.INITED)){
                Exception ex = ServiceOperations.stopQuietly(LOG, service);
                if(ex != null && firstException == null){
                    firstException = ex;
                }
            }
        }

        if(firstException != null){
            throw ServiceStateException.convert(firstException);
        }
    }

    @Override
    protected void serviceInit() throws Exception {
        List<Service> services = getServices();
        if (LOG.isDebugEnabled()) {
            LOG.debug(getName() + ": initing services, size=" + services.size());
        }
        for (Service service : services) {
            service.init();
        }
        super.serviceInit();
    }
}
