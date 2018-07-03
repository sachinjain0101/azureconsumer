package com.bullhorn.services;

import com.bullhorn.app.Constants;
import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.refreshWork.dao.ServiceBusMessagesDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

@Service
public class SwapperHandler {
    @Autowired
    @Qualifier("swapperTaskScheduler")
    ThreadPoolTaskScheduler taskScheduler;

    private ServiceBusMessagesDAO serviceBusMessagesDAO;
    private AzureConsumerDAO azureConsumerDAO;

    private long interval;
    private int poolSize;

    Map<CancellableRunnable, Future<?>> cancellableFutures = new HashMap<>();

    public void setInterval(long interval) {
        this.interval = interval;
    }

    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
    }

    @Autowired
    public SwapperHandler(ServiceBusMessagesDAO serviceBusMessagesDAO, AzureConsumerDAO azureConsumerDAO) {
        this.serviceBusMessagesDAO = serviceBusMessagesDAO;
        this.azureConsumerDAO = azureConsumerDAO;
    }

    public void executeAsynchronously() {
        //taskScheduler.scheduleWithFixedDelay(new Swapper(serviceBusMessagesDAO,azureConsumerDAO),interval);
        for (int i = 1; i <= poolSize; i++) {
            Swapper swapper = new Swapper(serviceBusMessagesDAO, azureConsumerDAO, interval);
            taskScheduler.setThreadNamePrefix(Constants.DATA_SWAPPER_THREAD_POOL_PREFIX);
            Future<?> future = taskScheduler.submit(swapper);
            cancellableFutures.put(swapper, future);
        }
    }

    public void shutdown() {

        for (Map.Entry<CancellableRunnable, Future<?>> entry : cancellableFutures.entrySet()) {
            entry.getKey().cancel();
            entry.getValue().cancel(true);
        }

        taskScheduler.shutdown();
    }
}
