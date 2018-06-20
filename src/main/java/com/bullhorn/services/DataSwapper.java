package com.bullhorn.services;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.inmem.model.TblAzureConsumer;
import com.bullhorn.orm.refreshWork.dao.ServiceBusMessagesDAO;
import com.bullhorn.orm.refreshWork.model.TblIntegrationServiceBusMessages;
import com.google.common.collect.Iterables;
import org.apache.commons.collections4.IterableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Component
public class DataSwapper{

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSwapper.class);

    ServiceBusMessagesDAO serviceBusMessagesDAO;
    AzureConsumerDAO azureConsumerDAO;

    @Autowired
    public DataSwapper(ServiceBusMessagesDAO serviceBusMessagesDAO, AzureConsumerDAO azureConsumerDAO){
        this.serviceBusMessagesDAO = serviceBusMessagesDAO;
        this.azureConsumerDAO = azureConsumerDAO;
    }

    @Scheduled(fixedDelay = 5000, initialDelay = 3000)
    public void run() {
        LOGGER.info("Running the Data Swapper");
        Iterable<TblAzureConsumer> tblAzureConsumers = azureConsumerDAO.findAll();

        if(Iterables.size(tblAzureConsumers)>0) {
            //LOGGER.info("Got some data to swap");
            List<TblIntegrationServiceBusMessages> tblIntegrationServiceBusMessages = StreamSupport.stream(tblAzureConsumers.spliterator(), false)
                    .map(m -> new TblIntegrationServiceBusMessages(m.getMessageID()
                            , m.getSequenceNumber()
                            , m.getIntegrationKey()
                            , m.getMessage()
                            , m.getFrontOfficeSystemRecordID()))
                    .collect(Collectors.toList());

            tblIntegrationServiceBusMessages.forEach((m)->LOGGER.info("{}",m.getMessage().length()));
            serviceBusMessagesDAO.batchInsert(tblIntegrationServiceBusMessages);
            azureConsumerDAO.deleteAll(tblAzureConsumers);
        }

    }
}
