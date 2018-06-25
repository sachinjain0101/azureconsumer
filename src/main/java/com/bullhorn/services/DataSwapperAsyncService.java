package com.bullhorn.services;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.refreshWork.dao.ServiceBusMessagesDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Service;

@Service
public class DataSwapperAsyncService{
    @Autowired
    @Qualifier("swapperTaskScheduler")
    ThreadPoolTaskScheduler taskScheduler;

    private ServiceBusMessagesDAO serviceBusMessagesDAO;
    private AzureConsumerDAO azureConsumerDAO;

    private long interval;

    public void setInterval(long interval) {
        this.interval = interval;
    }

    @Autowired
    public DataSwapperAsyncService(ServiceBusMessagesDAO serviceBusMessagesDAO, AzureConsumerDAO azureConsumerDAO) {
        this.serviceBusMessagesDAO = serviceBusMessagesDAO;
        this.azureConsumerDAO = azureConsumerDAO;
    }

    public void executeAsynchronously() {
        taskScheduler.scheduleWithFixedDelay(new DataSwapper(serviceBusMessagesDAO,azureConsumerDAO),interval);
    }
}
