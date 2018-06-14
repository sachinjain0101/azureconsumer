package com.bullhorn.services;

import com.bullhorn.json.model.AzureConfig;
import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.timecurrent.dao.ErrorsDAO;
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

    private AzureConfig config;
    private AzureConsumerDAO azureConsumerDAO;
    private ErrorsDAO errorsDAO;

    @Autowired
    public AzureConsumerAsyncService(@Qualifier("azureConfig") AzureConfig config, AzureConsumerDAO azureConsumerDAO, ErrorsDAO errorsDAO){
        this.config = config;
        this.azureConsumerDAO = azureConsumerDAO;
        this.errorsDAO = errorsDAO;
    }

    public List<QueueClient> executeAsynchronously() {

        List<TblIntegrationFrontOfficeSystem> lstFOS = config.getLstFOS();
        List<QueueClient> consumers = new ArrayList<>();

        for(TblIntegrationFrontOfficeSystem fos : lstFOS){
            AzureConsumer consumer = new AzureConsumer(fos,config.getTopicName(), azureConsumerDAO, errorsDAO);
            consumers.add(consumer.getReceiveClient());
            executor.execute(consumer);
        }

        return consumers;
    }

}
