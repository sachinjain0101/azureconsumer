package com.bullhorn;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.servicebus.primitives.ConnectionStringBuilder;
import com.microsoft.azure.servicebus.primitives.ServiceBusException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ReceiveLoop {

    static final Gson GSON = new Gson();




    public void run(String connectionString, String QueueName) throws Exception {

        QueueClient receiveClient = new QueueClient(new ConnectionStringBuilder(connectionString, QueueName), ReceiveMode.PEEKLOCK);
        this.registerReceiver(receiveClient);

        QueueClient sendClient = new QueueClient(new ConnectionStringBuilder(connectionString, "sachin-test"), ReceiveMode.PEEKLOCK);
        this.sendMessagesAsync(sendClient).thenRunAsync(() -> sendClient.closeAsync());

        this.waitForEnter(600);

        receiveClient.close();

    }


    void registerReceiver(QueueClient queueClient) throws Exception {

        // register the RegisterMessageHandler callback
        queueClient.registerMessageHandler(new IMessageHandler() {
                                               // callback invoked when the message handler loop has obtained a message
                                               public CompletableFuture<Void> onMessageAsync(IMessage message) {
                                                   // receives message is passed to callback
//                                                   if (message.getLabel() != null &&
//                                                           message.getContentType() != null &&
//                                                           message.getLabel().contentEquals("Scientist") &&
//                                                           message.getContentType().contentEquals("application/json")) {

                                                       byte[] body = message.getBody();
                                                       //Map scientist = GSON.fromJson(new String(body, UTF_8), Map.class);

                                                       System.out.printf("\n************* %s **************\n\n",new String(body, UTF_8));


//                                                       System.out.printf(
//                                                               "\n\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = %s, \n\t\t\t\t\t\tSequenceNumber = %s, \n\t\t\t\t\t\tEnqueuedTimeUtc = %s," +
//                                                                       "\n\t\t\t\t\t\tExpiresAtUtc = %s, \n\t\t\t\t\t\tContentType = \"%s\",  \n\t\t\t\t\t\tContent: [ firstName = %s, name = %s ]\n",
//                                                               message.getMessageId(),
//                                                               message.getSequenceNumber(),
//                                                               message.getEnqueuedTimeUtc(),
//                                                               message.getExpiresAtUtc(),
//                                                               message.getContentType(),
//                                                               scientist != null ? scientist.get("firstName") : "",
//                                                               scientist != null ? scientist.get("name") : "");
//                                                   }
                                                   return CompletableFuture.completedFuture(null);
                                               }

                                               // callback invoked when the message handler has an exception to report
                                               public void notifyException(Throwable throwable, ExceptionPhase exceptionPhase) {
                                                   System.out.printf(exceptionPhase + "-" + throwable.getMessage());
                                               }
                                           },
                // 1 concurrent call, messages are auto-completed, auto-renew duration
                new MessageHandlerOptions(10, true, Duration.ofMinutes(10)));

    }

    private CompletableFuture<Void> sendMessagesAsync(QueueClient sendClient) {

        List<HashMap<String, String>> data =
                GSON.fromJson(
                        "[" +
                                "{'name' = 'Einstein', 'firstName' = 'Albert'}"+
                                "]",
                        new TypeToken<List<HashMap<String, String>>>() {
                        }.getType());

        List<CompletableFuture> tasks = new ArrayList<>();
        for (int i = 0; i < data.size(); i++) {
            final String messageId = Integer.toString(i);
            Message message = new Message(GSON.toJson(data.get(i), Map.class).getBytes(UTF_8));
            message.setContentType("application/json");
            message.setLabel("Scientist");
            message.setMessageId(messageId);
            message.setTimeToLive(Duration.ofMinutes(2));

            tasks.add(
                    sendClient.sendAsync(message).thenRunAsync(() -> {
                        System.out.printf("Message sent: Id = %s\n", message.getMessageId());
                    }));
        }
        return CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[tasks.size()]));
    }

    public static void main(String[] args) {

        String connectionString = "Endpoint=sb://peoplenetdev.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=b8Ft7klNhqYwOeKHJ1eg1Ddwa+dgJgepVVMj+P1Lkcs=";
        String queueName="sachin-test";
        ReceiveLoop app = new ReceiveLoop();
        try {
            app.run(connectionString,queueName);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void waitForEnter(int seconds) {
        ExecutorService executor = Executors.newCachedThreadPool();
        try {
            executor.invokeAny(Arrays.asList(() -> {
                System.in.read();
                return 0;
            }, () -> {
                Thread.sleep(seconds * 1000);
                return 0;
            }));
        } catch (Exception e) {
            // absorb
        }
    }
}