package com.bullhorn.services;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.inmem.model.TblAzureConsumer;
import com.bullhorn.orm.refreshWork.dao.ServiceBusMessagesDAO;
import com.bullhorn.orm.refreshWork.model.TblIntegrationServiceBusMessages;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DataSwapper implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(DataSwapper.class);

    private ServiceBusMessagesDAO serviceBusMessagesDAO;
    private AzureConsumerDAO azureConsumerDAO;

    public DataSwapper(ServiceBusMessagesDAO serviceBusMessagesDAO, AzureConsumerDAO azureConsumerDAO){
        this.serviceBusMessagesDAO = serviceBusMessagesDAO;
        this.azureConsumerDAO = azureConsumerDAO;
    }

    public void run() {
        LOGGER.debug("Running the Data Swapper");
        Iterable<TblAzureConsumer> tblAzureConsumers = azureConsumerDAO.findAll();

        if(Iterables.size(tblAzureConsumers)>0) {
            //LOGGER.debug("Got some data to swap");
            List<TblIntegrationServiceBusMessages> tblIntegrationServiceBusMessages = StreamSupport.stream(tblAzureConsumers.spliterator(), false)
                    .map(m -> new TblIntegrationServiceBusMessages(m.getMessageID()
                            , m.getSequenceNumber()
                            , m.getIntegrationKey()
                            , m.getMessage()
                            , m.getFrontOfficeSystemRecordID()))
                    .collect(Collectors.toList());

            tblIntegrationServiceBusMessages.forEach((m)->LOGGER.debug("{}",m.getMessage().length()));
            serviceBusMessagesDAO.batchInsert(tblIntegrationServiceBusMessages);
            azureConsumerDAO.deleteAll(tblAzureConsumers);
        }

    }
}
