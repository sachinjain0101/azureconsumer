package com.bullhorn.orm.inmem.model;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Lob;
import java.io.Serializable;
import java.util.Objects;

@Entity
public class TblAzureConsumer implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    private String messageId;
    @Lob
    @Column(name="CONTENT", length=5000000)
    private String message;
    private Long sequenceNumber;
    private String client;
    private String integrationKey;

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(Long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getClient() {
        return client;
    }

    public void setClient(String client) {
        this.client = client;
    }

    public String getIntegrationKey() {
        return integrationKey;
    }

    public void setIntegrationKey(String integrationKey) {
        this.integrationKey = integrationKey;
    }

    public TblAzureConsumer(String messageId, String client, String integrationKey) {
        this.messageId = messageId;
        this.client = client;
        this.integrationKey = integrationKey;
    }

    public TblAzureConsumer(String messageId, String message, String client, String integrationKey) {
        this.messageId = messageId;
        this.message = message;
        this.client = client;
        this.integrationKey = integrationKey;
    }

    public TblAzureConsumer(String messageId, String message) {
        this.messageId = messageId;
        this.message = message;
    }

    public TblAzureConsumer(String messageId, String message, Long sequenceNumber) {
        this.messageId = messageId;
        this.message = message;
        this.sequenceNumber = sequenceNumber;
    }

    public TblAzureConsumer() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TblAzureConsumer that = (TblAzureConsumer) o;
        return Objects.equals(messageId, that.messageId) &&
                Objects.equals(message, that.message) &&
                Objects.equals(client, that.client) &&
                Objects.equals(integrationKey, that.integrationKey);
    }

    @Override
    public int hashCode() {

        return Objects.hash(messageId, message, client, integrationKey);
    }

    @Override
    public String toString() {
        return "TblAzureConsumer{" +
                "messageId='" + messageId + '\'' +
                ", client='" + client + '\'' +
                ", integrationKey='" + integrationKey + '\'' +
                ", sequenceNumber='" + sequenceNumber + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
