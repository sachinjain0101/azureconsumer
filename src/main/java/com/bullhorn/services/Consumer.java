package com.bullhorn.services;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.inmem.model.TblAzureConsumer;
import com.bullhorn.orm.timecurrent.model.TblIntegrationFrontOfficeSystem;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

public class Consumer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Consumer.class);
    private static final String INTEGRATION_KEY = "IntegrationKey";
    private TblIntegrationFrontOfficeSystem fos;
    private QueueClient receiveClient;
    private final String topicName;
    private final AzureConsumerDAO azureConsumerDAO;

    public Consumer(TblIntegrationFrontOfficeSystem fos, String topicName, AzureConsumerDAO azureConsumerDAO) {
        this.fos = fos;
        this.topicName = topicName;
        this.azureConsumerDAO = azureConsumerDAO;
        setReceiveClient();
    }

    public QueueClient getReceiveClient() {
        return receiveClient;
    }

    private void setReceiveClient() {
        try {
            this.receiveClient = new QueueClient(new ConnectionStringBuilder(fos.getAzureEndPoint(), topicName), ReceiveMode.PEEKLOCK);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ServiceBusException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        LOGGER.debug("Consumer is running for {}", fos.getName());
        try {
            registerReceiver(receiveClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void registerReceiver(QueueClient queueClient) throws Exception {
        queueClient.registerMessageHandler(new IMessageHandler() {
                                               // callback invoked when the message handler loop has obtained a message
                                               public CompletableFuture<Void> onMessageAsync(IMessage message) {

                                                   String receivedMessage = new String(message.getBody(), UTF_8);
                                                   String messageId = message.getMessageId();
                                                   Long sequenceNumber = message.getSequenceNumber();

                                                   String integrationKey = "";
                                                   if (message.getProperties()!=null && message.getProperties().containsKey(INTEGRATION_KEY))
                                                       integrationKey = message.getProperties().get(INTEGRATION_KEY);

                                                   LOGGER.debug("{}\t{}", sequenceNumber, receivedMessage);
                                                   azureConsumerDAO.save(new TblAzureConsumer(messageId, sequenceNumber, integrationKey, receivedMessage, fos.getRecordId()));

                                                   return CompletableFuture.completedFuture(null);
                                               }

                                               // callback invoked when the message handler has an exception to report
                                               public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                                                   LOGGER.error(exceptionPhase + "-" + throwable.getMessage());
                                               }
                                           },
                // 1 concurrent call, messages are auto-completed, auto-renew duration
                new MessageHandlerOptions(50, true, Duration.ofMinutes(10)));
    }

}
