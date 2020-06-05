package com.knowlegene.parent.scheduler.event;

import com.knowlegene.parent.scheduler.service.AbstractService;
import com.knowlegene.parent.scheduler.service.ServiceStateException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @Classname AsyncDispatcher
 * @Description TODO
 * @Date 2020/6/5 15:49
 * @Created by limeng
 */
public class AsyncDispatcher extends AbstractService implements Dispatcher {
    private static final Log LOG = LogFactory.getLog(AsyncDispatcher.class);

    private final BlockingQueue<Event> eventQueue;
    private volatile boolean stopped = false;

    private volatile boolean drainEventsOnStop = false;

    private volatile boolean drained = true;

    private Object waitForDrained = new Object();


    private volatile boolean blockNewEvents = false;


    private EventHandler handlerInstance = null;

    private Thread eventHandlingThread;

    protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;

    private boolean exitOnDispatchException;

    public AsyncDispatcher(){
        this(new LinkedBlockingQueue<Event>());
    }

    public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
        super("Dispatcher");
        this.eventQueue = eventQueue;
        this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
    }


    @Override
    protected void serviceStart() throws Exception {
        super.serviceStart();
        eventHandlingThread = new Thread(createThread());
        eventHandlingThread.setName("AsyncDispatcher event handler");
        eventHandlingThread.start();
    }

    @Override
    protected void serviceStop() throws Exception {
        if(drainEventsOnStop){
            blockNewEvents = true;
            synchronized (waitForDrained){
                while (!drained && eventHandlingThread.isAlive()){
                    waitForDrained.wait(1000);
                    LOG.info("Waiting for AsyncDispatcher to drain.");
                }
            }
        }

        stopped = true;
        if (eventHandlingThread != null) {
            eventHandlingThread.interrupt();
            try {
                eventHandlingThread.join();
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted Exception while stopping", ie);
            }

        }

        super.serviceStop();
    }



    private  Runnable createThread(){
        return ()->{
            while (!stopped && !Thread.currentThread().isInterrupted()){
                drained = eventQueue.isEmpty();

                if(blockNewEvents){
                    if (drained) {
                        waitForDrained.notify();
                    }
                }

                Event event;
                try {
                    event = eventQueue.take();
                }catch (InterruptedException ie){
                    if(!stopped){
                        LOG.warn("AsyncDispatcher thread interrupted", ie);
                    }
                    return;
                }

                if (event != null) {
                    dispatch(event);
                }

            }
        };
    }


    protected void dispatch(Event event) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("Dispatching the event " + event.getClass().getName() + "."
                    + event.toString());
        }

        Class<? extends Enum> type = event.getType().getDeclaringClass();
        try {
            EventHandler handler = eventDispatchers.get(type);
            if(handler != null) {
                handler.handle(event);
            } else {
                throw new Exception("No handler for registered for " + type);
            }
        }catch (Throwable t){
            LOG.fatal("Error in dispatcher thread", t);
            if (exitOnDispatchException
                    && stopped == false) {
                LOG.info("Exiting, bbye..");
                System.exit(-1);
            }
        }

    }

    @Override
    public EventHandler getEventHandler() {
        if (handlerInstance == null) {
            handlerInstance = new GenericEventHandler();
        }
        return handlerInstance;
    }

    @Override
    public void register(Class<? extends Enum> eventType, EventHandler handler) {
        EventHandler<Event> registeredHandler = (EventHandler<Event>) eventDispatchers.get(eventType);

        LOG.info("Registering " + eventType + " for " + handler.getClass());
        if(registeredHandler == null){
            eventDispatchers.put(eventType,handler);

        }else if(!(registeredHandler instanceof MultiListenerHandler)){
            MultiListenerHandler multiHandler = new MultiListenerHandler();
            multiHandler.addHandler(registeredHandler);
            multiHandler.addHandler(handler);
            eventDispatchers.put(eventType, multiHandler);
        }else{
            MultiListenerHandler multiHandler
                    = (MultiListenerHandler) registeredHandler;
            multiHandler.addHandler(handler);
        }

    }


    public void setDrainEventsOnStop() {
        drainEventsOnStop = true;
    }


    class  GenericEventHandler implements EventHandler<Event> {

        @Override
        public void handle(Event event) {
            if(blockNewEvents){
                return;
            }
            drained = false;

            int qSize = eventQueue.size();
            if(qSize !=0 && qSize %1000 == 0){
                LOG.info("Size of event-queue is " + qSize);
            }

            int remCapacity = eventQueue.remainingCapacity();

            if(remCapacity < 1000){
                LOG.warn("Very low remaining capacity in the event-queue: "
                        + remCapacity);
            }

            try {
                eventQueue.put(event);
            }catch (InterruptedException e){
                if (!stopped) {
                    LOG.warn("AsyncDispatcher thread interrupted", e);
                }
                throw new ServiceStateException(e);
            }

        }
    }

    static class MultiListenerHandler implements EventHandler<Event> {
        List<EventHandler<Event>> listofHandlers;

        public MultiListenerHandler() {
            listofHandlers = new ArrayList<EventHandler<Event>>();
        }

        @Override
        public void handle(Event event) {
            for (EventHandler<Event> handler: listofHandlers) {
                handler.handle(event);
            }
        }

        void addHandler(EventHandler<Event> handler) {
            listofHandlers.add(handler);
        }
    }


}
