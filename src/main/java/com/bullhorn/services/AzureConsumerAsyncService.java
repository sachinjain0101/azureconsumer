package com.bullhorn.services;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.timecurrent.model.TblIntegrationFrontOfficeSystem;
import com.microsoft.azure.servicebus.QueueClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.task.TaskExecutor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class AzureConsumerAsyncService {

    @Autowired
    @Qualifier("consumerTaskExecutor")
    TaskExecutor executor;

    private AzureConsumerDAO azureConsumerDAO;
    private List<TblIntegrationFrontOfficeSystem> lstFOS;
    private String queueName;

    @Autowired
    public AzureConsumerAsyncService(AzureConsumerDAO azureConsumerDAO){
        this.azureConsumerDAO = azureConsumerDAO;
    }

    public void setLstFOS(List<TblIntegrationFrontOfficeSystem> lstFOS) {
        this.lstFOS = lstFOS;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public List<QueueClient> executeAsynchronously() {

        List<QueueClient> consumers = new ArrayList<>();

        for(TblIntegrationFrontOfficeSystem fos : lstFOS){
            AzureConsumer consumer = new AzureConsumer(fos,queueName, azureConsumerDAO);
            consumers.add(consumer.getReceiveClient());
            executor.execute(consumer);
        }

        return consumers;
    }

}
