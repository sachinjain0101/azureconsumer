package com.bullhorn.services;

import com.bullhorn.orm.inmem.dao.AzureConsumerDAO;
import com.bullhorn.orm.inmem.model.TblAzureConsumer;
import com.bullhorn.orm.timecurrent.dao.ErrorsDAO;
import com.bullhorn.orm.timecurrent.model.TblIntegrationFrontOfficeSystem;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.nio.charset.StandardCharsets.UTF_8;

public class AzureConsumer implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AzureConsumer.class);

    private TblIntegrationFrontOfficeSystem fos;
    private QueueClient receiveClient;
    private String topicName;
    private AzureConsumerDAO azureConsumerDAO;
    private ErrorsDAO errorsDAO;

    public AzureConsumer(TblIntegrationFrontOfficeSystem fos, String topicName, AzureConsumerDAO azureConsumerDAO, ErrorsDAO errorsDAO) {
        this.fos = fos;
        this.topicName = topicName;
        this.azureConsumerDAO = azureConsumerDAO;
        this.errorsDAO = errorsDAO;
        setReceiveClient();
    }

    public QueueClient getReceiveClient() {
        return receiveClient;
    }

    public void setReceiveClient() {
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
        LOGGER.info("AzureConsumer is running for {}", fos.getName());
        try {
            consumers = new ArrayList<>();
            registerReceiver(receiveClient);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private List<TblAzureConsumer> consumers;


    private void registerReceiver(QueueClient queueClient) throws Exception {
        queueClient.registerMessageHandler(new IMessageHandler() {
                                               // callback invoked when the message handler loop has obtained a message
                                               public CompletableFuture<Void> onMessageAsync(IMessage message) {

                                                   String receivedMessage = new String(message.getBody(), UTF_8);
                                                   String messageId = message.getMessageId();
                                                   Long sequenceNumber = message.getSequenceNumber();

                                                   LOGGER.info("{}\t{}", sequenceNumber, receivedMessage);
                                                   azureConsumerDAO.save(new TblAzureConsumer(messageId, sequenceNumber, receivedMessage, fos.getRecordId()));
                                                   return CompletableFuture.completedFuture(null);
                                               }

                                               // callback invoked when the message handler has an exception to report
                                               public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                                                   System.out.printf(exceptionPhase + "-" + throwable.getMessage());
                                               }
                                           },
                // 1 concurrent call, messages are auto-completed, auto-renew duration
                new MessageHandlerOptions(100, true, Duration.ofMinutes(10)));
    }

}
