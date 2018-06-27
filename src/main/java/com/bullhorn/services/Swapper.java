package com.bullhorn.services;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.inmem.model.TblAzureConsumer;
import com.bullhorn.orm.refreshWork.dao.ServiceBusMessagesDAO;
import com.bullhorn.orm.refreshWork.model.TblIntegrationServiceBusMessages;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Swapper implements CancellableRunnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Swapper.class);

    private final ServiceBusMessagesDAO serviceBusMessagesDAO;
    private final AzureConsumerDAO azureConsumerDAO;
    public final long interval;

    private AtomicBoolean processing = new AtomicBoolean();

    public Swapper(ServiceBusMessagesDAO serviceBusMessagesDAO, AzureConsumerDAO azureConsumerDAO, long interval) {
        this.serviceBusMessagesDAO = serviceBusMessagesDAO;
        this.azureConsumerDAO = azureConsumerDAO;
        this.interval = interval;
    }

    @Override
    public void run() {
        processing.set(true);
        LOGGER.debug("Running the Data Swapper : {}", processing.get());

        while (!Thread.interrupted() && processing.get()) {

            LOGGER.debug("Start - {}", new Date().toString());
            Iterable<TblAzureConsumer> tblAzureConsumers = azureConsumerDAO.findAll();
            List<TblIntegrationServiceBusMessages> tblIntegrationServiceBusMessages = new ArrayList<>();

            if (Iterables.size(tblAzureConsumers) > 0) {
                LOGGER.debug("Got some data to swap");
                tblIntegrationServiceBusMessages = StreamSupport.stream(tblAzureConsumers.spliterator(), false)
                        .map(m -> new TblIntegrationServiceBusMessages(m.getMessageID()
                                , m.getSequenceNumber()
                                , m.getIntegrationKey()
                                , m.getMessage()
                                , m.getFrontOfficeSystemRecordID()))
                        .collect(Collectors.toList());

                tblIntegrationServiceBusMessages.forEach((m) -> LOGGER.debug("{}", m.getMessage().length()));
                serviceBusMessagesDAO.batchInsert(tblIntegrationServiceBusMessages);
                azureConsumerDAO.deleteAll(tblAzureConsumers);
            }

            /*
            // Test Block
            List<String> lst = new ArrayList<>();
            for(int i=1;i<=10000;i++){
                String s = new Date().toString();
                //LOGGER.debug("{}",s);
                lst.add(s);
            }
            serviceBusMessagesDAO.batchInsertTest(lst);
            */
            LOGGER.debug("End - {}", new Date().toString());

            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                LOGGER.debug("Data Swapper interrupted : {}", e.getMessage());
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }

    }

    @Override
    public void cancel() {
        processing.set(false);
        LOGGER.debug("Stopping the Data Swapper : {}", processing.get());
    }
}
